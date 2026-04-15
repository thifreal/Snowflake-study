"""
Main ETL Pipeline Orchestration Script
Orchestrates the execution of Bronze -> Silver -> Gold layers
"""
import os
import sys
import logging
import argparse
from pathlib import Path
from typing import Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.python.utils.logging_utils import setup_logging, get_logger
from config.connections import SnowflakeConnector, ConfigLoader

logger = get_logger(__name__)


class SnowflakeETL:
    """Main ETL orchestration class"""
    
    def __init__(self, config_path: str, credentials_path: str):
        """
        Initialize ETL pipeline
        
        Args:
            config_path: Path to environment config file
            credentials_path: Path to credentials file
        """
        self.config = ConfigLoader.load_config(config_path)
        self.credentials = ConfigLoader.load_credentials(credentials_path)
        self.connector = SnowflakeConnector(self.credentials)
        
    def execute_layer(self, layer: str, dry_run: bool = False) -> bool:
        """
        Execute specific layer (bronze, silver, gold)
        
        Args:
            layer: Layer to execute ('bronze', 'silver', 'gold')
            dry_run: If True, validate without executing
            
        Returns:
            True if execution successful, False otherwise
        """
        logger.info(f"Starting {layer.upper()} layer execution (dry_run={dry_run})")
        
        try:
            # Get SQL files for layer
            sql_dir = Path(__file__).parent.parent / 'sql' / layer
            sql_files = sorted(sql_dir.glob('*.sql'))
            
            if not sql_files:
                logger.warning(f"No SQL files found for {layer} layer in {sql_dir}")
                return False
            
            self.connector.connect()
            
            for sql_file in sql_files:
                logger.info(f"Executing: {sql_file.name}")
                
                with open(sql_file, 'r') as f:
                    sql_content = f.read()
                
                if not dry_run:
                    self.connector.execute_script(str(sql_file))
                    logger.info(f"✓ Successfully executed {sql_file.name}")
                else:
                    logger.info(f"[DRY RUN] Would execute {sql_file.name}")
            
            logger.info(f"✓ {layer.upper()} layer execution completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Error executing {layer} layer: {str(e)}", exc_info=True)
            return False
        finally:
            self.connector.disconnect()
    
    def execute_full_pipeline(self, dry_run: bool = False) -> bool:
        """
        Execute complete ETL pipeline (bronze -> silver -> gold)
        
        Args:
            dry_run: If True, validate without executing
            
        Returns:
            True if all layers successful, False otherwise
        """
        logger.info("=" * 60)
        logger.info("Starting Full ETL Pipeline Execution")
        logger.info("=" * 60)
        
        layers = ['bronze', 'silver', 'gold']
        results = {}
        
        for layer in layers:
            # Check if layer is enabled in config
            if not self.config.get('pipeline', {}).get(layer, {}).get('enabled', True):
                logger.info(f"⊘ {layer.upper()} layer is disabled in config")
                results[layer] = None
                continue
            
            results[layer] = self.execute_layer(layer, dry_run)
            
            if not results[layer]:
                logger.error(f"Pipeline failed at {layer} layer")
                break
        
        # Print summary
        logger.info("=" * 60)
        logger.info("Pipeline Execution Summary")
        logger.info("=" * 60)
        for layer, result in results.items():
            status = "✓ SUCCESS" if result else ("✗ FAILED" if result is False else "⊘ SKIPPED")
            logger.info(f"{layer.upper():10} : {status}")
        
        return all(v for v in results.values() if v is not None)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Snowflake ETL Pipeline Orchestrator'
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
        help='Path to credentials file (default: config/credentials.yaml)'
    )
    parser.add_argument(
        '--layer',
        type=str,
        choices=['bronze', 'silver', 'gold'],
        help='Execute specific layer (default: all layers)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate pipeline without executing queries'
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
    log_file = f"logs/etl_{args.layer or 'full'}.log"
    setup_logging(args.log_level, log_file)
    
    try:
        # Verify config and credentials exist
        if not os.path.exists(args.config):
            logger.error(f"Config file not found: {args.config}")
            return 1
        
        if not os.path.exists(args.credentials):
            logger.error(f"Credentials file not found: {args.credentials}")
            logger.info(f"Please create {args.credentials} with your Snowflake credentials")
            return 1
        
        # Initialize and run ETL
        etl = SnowflakeETL(args.config, args.credentials)
        
        if args.layer:
            success = etl.execute_layer(args.layer, args.dry_run)
        else:
            success = etl.execute_full_pipeline(args.dry_run)
        
        return 0 if success else 1
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
