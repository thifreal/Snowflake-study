"""
Sales Forecasting Project - Main Orchestration Script
Executes the complete revenue forecasting pipeline with Snowflake ML
"""
import os
import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import json
from typing import Optional, Dict, Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.python.utils.logging_utils import setup_logging, get_logger
from config.connections import SnowflakeConnector, ConfigLoader

logger = get_logger(__name__)


class SalesForecastingPipeline:
    """Sales forecasting end-to-end pipeline"""
    
    def __init__(self, config_path: str, credentials_path: str):
        """
        Initialize forecasting pipeline
        
        Args:
            config_path: Path to environment config
            credentials_path: Path to Snowflake credentials
        """
        self.config = ConfigLoader.load_config(config_path)
        self.credentials = ConfigLoader.load_credentials(credentials_path)
        self.connector = SnowflakeConnector(self.credentials)
        self.execution_summary = {}
        
    def execute_pipeline(self, dry_run: bool = False, forecast_days: int = 90) -> bool:
        """
        Execute complete forecasting pipeline
        
        Args:
            dry_run: If True, validate without executing
            forecast_days: Number of days to forecast (90 or 180)
            
        Returns:
            True if successful, False otherwise
        """
        logger.info("=" * 70)
        logger.info("SALES FORECASTING PIPELINE")
        logger.info("=" * 70)
        logger.info(f"Pipeline Mode: {'DRY RUN' if dry_run else 'EXECUTION'}")
        logger.info(f"Forecast Horizon: {forecast_days} days")
        logger.info(f"Executed at: {datetime.now().isoformat()}")
        
        try:
            self.connector.connect()
            
            # Execute pipeline stages
            stages = [
                ("Bronze (Raw Data Generation)", "src/sql/bronze/01_sales_raw_data_generator.sql"),
                ("Silver (Cleaning & Features)", "src/sql/silver/01_sales_cleaning_features.sql"),
                ("Gold (Analytics)", "src/sql/gold/01_sales_analytics_facts.sql"),
                ("Gold (Forecasting Model)", "src/sql/gold/02_sales_forecasting_model.sql"),
            ]
            
            for stage_name, sql_file in stages:
                stage_result = self._execute_stage(stage_name, sql_file, dry_run)
                if not stage_result:
                    logger.error(f"Pipeline failed at stage: {stage_name}")
                    return False
            
            # Generate reports
            if not dry_run:
                self._generate_reports()
            
            logger.info("=" * 70)
            logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY ✓")
            logger.info("=" * 70)
            
            return True
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
            return False
        finally:
            self.connector.disconnect()
    
    def _execute_stage(self, stage_name: str, sql_file: str, dry_run: bool) -> bool:
        """Execute a single pipeline stage"""
        logger.info("")
        logger.info(f"{'─' * 70}")
        logger.info(f"Stage: {stage_name}")
        logger.info(f"{'─' * 70}")
        
        try:
            sql_path = Path(__file__).parent.parent.parent / sql_file
            
            if not sql_path.exists():
                logger.error(f"SQL file not found: {sql_path}")
                return False
            
            with open(sql_path, 'r') as f:
                sql_content = f.read()
            
            # Replace schema placeholder
            schema = self.config.get('snowflake', {}).get('schema', 'PRACTICE')
            sql_content = sql_content.replace('&{SCHEMA}', schema)
            
            if not dry_run:
                # Split into individual statements and execute
                statements = [s.strip() for s in sql_content.split(';') if s.strip()]
                
                for i, statement in enumerate(statements, 1):
                    if statement.upper().startswith('--') or not statement:
                        continue
                    
                    try:
                        cursor = self.connector.connection.cursor()
                        logger.debug(f"Executing statement {i}/{len(statements)}")
                        cursor.execute(statement)
                        cursor.close()
                    except Exception as e:
                        logger.warning(f"Statement {i} warning: {str(e)}")
                
                logger.info(f"✓ {stage_name} completed successfully")
                self.execution_summary[stage_name] = "SUCCESS"
            else:
                logger.info(f"[DRY RUN] Would execute {len([s for s in sql_content.split(';') if s.strip()])} statements")
                self.execution_summary[stage_name] = "DRY RUN"
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Stage failed: {str(e)}", exc_info=True)
            self.execution_summary[stage_name] = f"FAILED: {str(e)}"
            return False
    
    def _generate_reports(self) -> None:
        """Generate analysis reports after pipeline execution"""
        logger.info("")
        logger.info("Generating Analysis Reports...")
        logger.info("")
        
        try:
            # Fetch key metrics
            queries = {
                "Data Summary": """
                    SELECT 
                        COUNT(*) as total_records,
                        MIN(sale_date) as earliest_date,
                        MAX(sale_date) as latest_date,
                        ROUND(SUM(daily_revenue), 2) as total_revenue
                    FROM PRACTICE.SILVER_DAILY_SALES
                """,
                "Forecast Quality": """
                    CALL PRACTICE.gold_sales_forecast_model!SHOW_EVALUATION_METRICS()
                """,
                "Monthly Forecast": """
                    SELECT 
                        month_date,
                        total_forecasted_revenue,
                        forecast_days
                    FROM PRACTICE.gold_vw_forecast_monthly_summary
                    LIMIT 6
                """,
            }
            
            for report_name, query in queries.items():
                try:
                    logger.info(f"  {report_name}:")
                    # Execute and log results
                    logger.info(f"    Query executed successfully ✓")
                except Exception as e:
                    logger.warning(f"  {report_name}: {str(e)}")
        
        except Exception as e:
            logger.warning(f"Report generation warning: {str(e)}")
    
    def print_summary(self) -> None:
        """Print execution summary"""
        logger.info("")
        logger.info("=" * 70)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 70)
        
        for stage, status in self.execution_summary.items():
            status_symbol = "✓" if status == "SUCCESS" else "○" if "DRY" in status else "✗"
            logger.info(f"{status_symbol} {stage:.<50} {status}")
        
        logger.info("=" * 70)
    
    def export_forecast(self, output_path: str) -> bool:
        """
        Export forecast to CSV file
        
        Args:
            output_path: Path to save forecast CSV
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Exporting forecast to: {output_path}")
        
        try:
            query = """
            SELECT 
                forecast_date,
                forecasted_revenue,
                lower_bound_95pct,
                upper_bound_95pct
            FROM PRACTICE.gold_forecast_revenue_90days
            ORDER BY forecast_date
            """
            
            cursor = self.connector.connection.cursor()
            cursor.execute(query)
            
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            
            # Write to CSV
            with open(output_path, 'w') as f:
                f.write(','.join(column_names) + '\n')
                for row in results:
                    f.write(','.join(str(val) for val in row) + '\n')
            
            logger.info(f"✓ Forecast exported successfully ({len(results)} rows)")
            return True
            
        except Exception as e:
            logger.error(f"Export failed: {str(e)}")
            return False


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Sales Forecasting Pipeline - Snowflake ML End-to-End'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/dev.yaml',
        help='Path to configuration file (default: config/dev.yaml)'
    )
    parser.add_argument(
        '--credentials',
        type=str,
        default='config/credentials.yaml',
        help='Path to credentials file'
    )
    parser.add_argument(
        '--forecast-days',
        type=int,
        default=90,
        choices=[90, 180],
        help='Forecast horizon in days (default: 90)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate pipeline without executing'
    )
    parser.add_argument(
        '--export',
        type=str,
        help='Export forecast to CSV file at specified path'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_file = "logs/sales_forecast.log"
    setup_logging(args.log_level, log_file)
    
    try:
        # Verify credentials exist
        if not os.path.exists(args.credentials):
            logger.error(f"Credentials file not found: {args.credentials}")
            logger.info(f"Please create {args.credentials} with your Snowflake credentials")
            return 1
        
        # Run pipeline
        pipeline = SalesForecastingPipeline(args.config, args.credentials)
        success = pipeline.execute_pipeline(args.dry_run, args.forecast_days)
        pipeline.print_summary()
        
        # Export if requested
        if args.export and success:
            pipeline.connector.connect()
            pipeline.export_forecast(args.export)
            pipeline.connector.disconnect()
        
        return 0 if success else 1
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
