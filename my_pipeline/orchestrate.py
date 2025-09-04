from prefect import task, flow, get_run_logger
from my_pipeline.extract import run_extraction
from my_pipeline.visualize import generate_visualization
import subprocess

@task(retries=1, retry_delay_seconds=10)
def extract_and_load_task():
    """Prefect task to run the extraction and loading script."""
    logger = get_run_logger()
    logger.info("Starting data extraction and loading...")
    try:
        run_extraction()
        logger.info("Extraction and loading completed successfully.")
    except Exception as e:
        logger.error(f"Extraction and loading failed: {e}")
        raise

@task
def transform_and_test_task():
    """Prefect task to run dbt build for transformation and quality checks."""
    logger = get_run_logger()
    logger.info("Starting data transformation and quality checks...")
    try:
        import os, sys
        project_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "my_crypto_pipeline")
        logger.info(f"Using project directory: {project_dir}")
        dbt_cmd = [sys.executable, "-m", "dbt", "build", "--project-dir", project_dir, "--profiles-dir", project_dir]
        env = os.environ.copy()
        result = subprocess.run(
            dbt_cmd,
            check=True,
            capture_output=True,
            text=True,
            cwd=project_dir,
            env=env
        )
        logger.info("Transformation and quality checks completed successfully.")
        logger.info(f"dbt output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"DBT build failed: {e.stderr}")
        logger.error(f"Return code: {e.returncode}")
        logger.error(f"Command: {e.cmd}")
        if e.stdout:
            logger.error(f"Stdout: {e.stdout}")
        raise

@task
def visualize_data_task():
    """Prefect task to generate and save a visualization."""
    logger = get_run_logger()
    logger.info("Starting data visualization...")
    try:
        generate_visualization()
        logger.info("Visualization task completed successfully.")
    except Exception as e:
        logger.error(f"Visualization failed: {e}")
        raise


@flow(name="Crypto ELT Pipeline - Production")
def run_perfected_crypto_pipeline():
    el_result = extract_and_load_task()
    transform_result = transform_and_test_task(wait_for=[el_result])
    visualize_data_task(wait_for=[transform_result])
    

if __name__ == "__main__":
    run_perfected_crypto_pipeline()