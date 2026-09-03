from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'owner': 'finshield_eng',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def check_snowflake_fraud_metrics(**kwargs):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Removed the strict 15-minute window since your demo data is historical (2019/2020)
    sql = """
        SELECT 
            COUNT(*) as high_risk_count,
            COALESCE(SUM(AMT), 0) as total_exposure_amount
        FROM FINSHIELD.SILVER_GOLD.FACT_TRANSACTIONS 
        WHERE IS_FRAUD = 1;
    """
    result = hook.get_first(sql)
    
    fraud_count = result[0] if result else 0
    exposure_amount = result[1] if result else 0.0
    
    print(f"Metrics evaluated: {fraud_count} high-risk transactions found, Total Exposure: ${exposure_amount}")
    
    ti = kwargs['ti']
    ti.xcom_push(key='fraud_count', value=fraud_count)
    ti.xcom_push(key='exposure_amount', value=exposure_amount)
    
    return fraud_count

def decide_workflow_branch(**kwargs):
    """Branches execution based on whether fraud volume exceeds operational threshold."""
    ti = kwargs['ti']
    fraud_count = ti.xcom_pull(task_ids='check_fraud_metrics', key='fraud_count')
    
    if fraud_count and fraud_count > 0:
        return 'send_internal_security_alert'
    return 'skip_alert'

with DAG(
    'finshield_internal_fraud_alert',
    default_args=default_args,
    description='Monitors Snowflake Gold layer and alerts internal team on fraud spikes',
    schedule_interval='@hourly',  # Runs every hour
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    check_fraud_metrics = PythonOperator(
        task_id='check_fraud_metrics',
        python_callable=check_snowflake_fraud_metrics,
        provide_context=True,
    )

    branch_decision = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=decide_workflow_branch,
        provide_context=True,
    )

    # Native Airflow EmailOperator using environment SMTP and Jinja templates
    send_alert = EmailOperator(
        task_id='send_internal_security_alert',
        to='gana.drk@gmail.com', 
        subject='🚨 [FinShield Security Ops] High-Risk Alert: {{ ti.xcom_pull(task_ids="check_fraud_metrics", key="fraud_count") }} Anomalies Flagged',
        html_content=""""
        <h3>[FinShield Automated Pipeline Surveillance]</h3>
        <p>The Apache Airflow monitoring DAG has evaluated the <b>Gold Layer Star Schema</b> and detected active fraud indicators:</p>
        
        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-family: Arial, sans-serif; text-align: left;">
            <tr style="background-color: #f2f2f2;">
                <th>Metric Description</th>
                <th>Value</th>
            </tr>
            <tr>
                <td><b>High-Risk Transactions Flagged</b></td>
                <td style="color: red; font-weight: bold;">{{ ti.xcom_pull(task_ids='check_fraud_metrics', key='fraud_count') }}</td>
            </tr>
            <tr>
                <td><b>Estimated Financial Exposure</b></td>
                <td>${{ ti.xcom_pull(task_ids='check_fraud_metrics', key='exposure_amount') }}</td>
            </tr>
            <tr>
                <td><b>Evaluation Window</b></td>
                <td>Last 15 Minutes</td>
            </tr>
        </table>
        
        <p><b>Required Action:</b> Please open your Power BI Live Dashboard to examine the Risk Heatmaps and transaction trends immediately.</p>
        <p><i>— FinShield Automated Data Pipeline Engine</i></p>
        """,
    )

    skip_alert = EmptyOperator(
        task_id='skip_alert',
    )

    # Workflow Pipeline Routing
    check_fraud_metrics >> branch_decision >> [send_alert, skip_alert]