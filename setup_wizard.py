#!/usr/bin/env python3
"""
Multi-cluster Kafka Manager Setup Wizard

This script provides an interactive setup wizard for installing and configuring
the multi-cluster Kafka manager system.
"""

import asyncio
import sys
import argparse
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.setup.multi_cluster_installer import (
    MultiClusterInstaller,
    InstallationType,
    DeploymentScenario,
    InstallationConfig
)


def print_banner():
    """Print welcome banner."""
    banner = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║               🚀 Multi-cluster Kafka Manager Setup Wizard                   ║
║                                                                              ║
║    Welcome to the interactive setup wizard for Multi-cluster Kafka Manager  ║
║    This wizard will guide you through the installation and configuration    ║
║    process to get your multi-cluster Kafka environment up and running.      ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
    print(banner)


def print_help():
    """Print help information."""
    help_text = """
🔧 Setup Wizard Commands:

  python setup_wizard.py                    # Run interactive installation wizard
  python setup_wizard.py --install          # Run installation with wizard
  python setup_wizard.py --validate         # Validate current installation
  python setup_wizard.py --backup           # Create backup of current installation
  python setup_wizard.py --restore <path>   # Restore from backup
  python setup_wizard.py --migrate          # Migrate from single-cluster
  python setup_wizard.py --repair           # Repair broken installation
  python setup_wizard.py --help             # Show this help message

🎯 Quick Start Examples:

  # Development setup (single cluster, minimal resources)
  python setup_wizard.py --quick-dev

  # Testing setup (multiple clusters for testing)
  python setup_wizard.py --quick-test

  # Production setup (production-ready configuration)
  python setup_wizard.py --quick-prod

📋 Configuration Options:

  --base-dir <path>         # Set base installation directory
  --data-dir <path>         # Set data directory
  --config-dir <path>       # Set configuration directory
  --port-range <start-end>  # Set port range (e.g., 9000-9999)
  --max-memory <mb>         # Set maximum memory limit
  --max-disk <gb>           # Set maximum disk limit
  --enable-auth             # Enable authentication
  --enable-ssl              # Enable SSL/TLS
  --enable-monitoring       # Enable resource monitoring
  --enable-backup           # Enable automatic backups

🔍 Validation and Maintenance:

  --check-requirements      # Check system requirements only
  --list-clusters           # List all configured clusters
  --cluster-status          # Show status of all clusters
  --cleanup                 # Run cleanup operations

For more information, visit: https://github.com/your-repo/multi-cluster-kafka-manager
"""
    print(help_text)


async def run_interactive_wizard():
    """Run the interactive installation wizard."""
    try:
        print_banner()
        
        installer = MultiClusterInstaller()
        
        print("🔍 Checking system requirements...")
        validation = await installer.validate_installation()
        
        if not validation.get("valid", False):
            print("⚠️  System validation issues detected:")
            for error in validation.get("errors", []):
                print(f"  ❌ {error}")
            
            proceed = input("\nDo you want to continue anyway? (y/N): ").lower().strip()
            if proceed != 'y':
                print("❌ Installation cancelled")
                return False
        
        # Run installation wizard
        result = await installer.install()
        
        return result.success
        
    except KeyboardInterrupt:
        print("\n❌ Installation cancelled by user")
        return False
    except Exception as e:
        print(f"\n❌ Installation failed: {e}")
        return False


async def run_quick_setup(scenario: DeploymentScenario, args):
    """Run quick setup for specific scenario."""
    try:
        print_banner()
        print(f"🚀 Running quick {scenario.value} setup...")
        
        installer = MultiClusterInstaller()
        
        # Create configuration based on scenario and args
        config = InstallationConfig(
            installation_type=InstallationType.FRESH_INSTALL,
            deployment_scenario=scenario,
            base_directory=Path(args.base_dir) if args.base_dir else Path.cwd(),
            data_directory=Path(args.data_dir) if args.data_dir else Path.cwd() / "data",
            config_directory=Path(args.config_dir) if args.config_dir else Path.cwd() / "config",
            enable_multi_cluster=True,
            max_memory_mb=args.max_memory if args.max_memory else 4096,
            max_disk_gb=args.max_disk if args.max_disk else 50,
            enable_authentication=args.enable_auth,
            enable_ssl=args.enable_ssl,
            enable_resource_monitoring=args.enable_monitoring,
            enable_backup=args.enable_backup
        )
        
        # Set port range if specified
        if args.port_range:
            try:
                start, end = map(int, args.port_range.split('-'))
                config.port_range_start = start
                config.port_range_end = end
            except ValueError:
                print(f"⚠️  Invalid port range format: {args.port_range}")
        
        # Run installation
        result = await installer.install(config)
        
        return result.success
        
    except Exception as e:
        print(f"❌ Quick setup failed: {e}")
        return False


async def validate_installation(args):
    """Validate current installation."""
    try:
        print("🔍 Validating installation...")
        
        installer = MultiClusterInstaller()
        validation = await installer.validate_installation()
        
        print(f"\n📋 Validation Results:")
        print(f"Overall Status: {'✅ Valid' if validation['valid'] else '❌ Invalid'}")
        print(f"Timestamp: {validation['timestamp']}")
        
        if validation.get("checks"):
            print(f"\n🔍 Detailed Checks:")
            for check_name, check_result in validation["checks"].items():
                status = "✅" if check_result.get("valid", True) else "❌"
                print(f"  {status} {check_name.replace('_', ' ').title()}")
        
        if validation.get("errors"):
            print(f"\n❌ Errors ({len(validation['errors'])}):")
            for error in validation["errors"]:
                print(f"  • {error}")
        
        if validation.get("warnings"):
            print(f"\n⚠️  Warnings ({len(validation['warnings'])}):")
            for warning in validation["warnings"]:
                print(f"  • {warning}")
        
        if validation.get("recommendations"):
            print(f"\n💡 Recommendations ({len(validation['recommendations'])}):")
            for recommendation in validation["recommendations"]:
                print(f"  • {recommendation}")
        
        return validation["valid"]
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False


async def create_backup(args):
    """Create backup of current installation."""
    try:
        print("💾 Creating backup...")
        
        installer = MultiClusterInstaller()
        backup_path = await installer.create_backup()
        
        print(f"✅ Backup created successfully at: {backup_path}")
        return True
        
    except Exception as e:
        print(f"❌ Backup failed: {e}")
        return False


async def restore_backup(backup_path: str):
    """Restore from backup."""
    try:
        print(f"🔄 Restoring from backup: {backup_path}")
        
        installer = MultiClusterInstaller()
        success = await installer.restore_backup(Path(backup_path))
        
        if success:
            print("✅ Backup restored successfully")
        else:
            print("❌ Backup restore failed")
        
        return success
        
    except Exception as e:
        print(f"❌ Restore failed: {e}")
        return False


async def migrate_from_single_cluster(args):
    """Migrate from single-cluster setup."""
    try:
        print("🔄 Migrating from single-cluster to multi-cluster...")
        
        installer = MultiClusterInstaller()
        
        # Look for existing docker-compose.yml
        docker_compose = Path("docker-compose.yml")
        if not docker_compose.exists():
            print("❌ No docker-compose.yml found for migration")
            return False
        
        result = await installer.migrate_from_single_cluster(docker_compose)
        
        if result.success:
            print("✅ Migration completed successfully")
        else:
            print(f"❌ Migration failed: {result.error_message}")
        
        return result.success
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        return False


async def repair_installation(args):
    """Repair broken installation."""
    try:
        print("🔧 Repairing installation...")
        
        installer = MultiClusterInstaller()
        result = await installer.repair_installation()
        
        if result.success:
            print("✅ Installation repaired successfully")
            if result.warnings:
                print("⚠️  Warnings:")
                for warning in result.warnings:
                    print(f"  • {warning}")
        else:
            print(f"❌ Repair failed: {result.error_message}")
        
        return result.success
        
    except Exception as e:
        print(f"❌ Repair failed: {e}")
        return False


async def check_requirements():
    """Check system requirements only."""
    try:
        print("🔍 Checking system requirements...")
        
        installer = MultiClusterInstaller()
        validation = await installer._validate_system_requirements()
        
        print(f"\n📋 System Requirements Check:")
        print(f"Status: {'✅ Passed' if validation['valid'] else '❌ Failed'}")
        
        if validation.get("checks"):
            print(f"\n✅ Available Components:")
            for component, version in validation["checks"].items():
                print(f"  • {component.replace('_', ' ').title()}: {version}")
        
        if validation.get("errors"):
            print(f"\n❌ Missing Requirements:")
            for error in validation["errors"]:
                print(f"  • {error}")
        
        if validation.get("warnings"):
            print(f"\n⚠️  Warnings:")
            for warning in validation["warnings"]:
                print(f"  • {warning}")
        
        return validation["valid"]
        
    except Exception as e:
        print(f"❌ Requirements check failed: {e}")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Multi-cluster Kafka Manager Setup Wizard",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python setup_wizard.py                    # Interactive wizard
  python setup_wizard.py --quick-dev        # Quick development setup
  python setup_wizard.py --validate         # Validate installation
  python setup_wizard.py --backup           # Create backup
        """
    )
    
    # Main commands
    parser.add_argument("--install", action="store_true", help="Run installation wizard")
    parser.add_argument("--validate", action="store_true", help="Validate current installation")
    parser.add_argument("--backup", action="store_true", help="Create backup")
    parser.add_argument("--restore", metavar="PATH", help="Restore from backup")
    parser.add_argument("--migrate", action="store_true", help="Migrate from single-cluster")
    parser.add_argument("--repair", action="store_true", help="Repair installation")
    parser.add_argument("--check-requirements", action="store_true", help="Check system requirements")
    
    # Quick setup options
    parser.add_argument("--quick-dev", action="store_true", help="Quick development setup")
    parser.add_argument("--quick-test", action="store_true", help="Quick testing setup")
    parser.add_argument("--quick-prod", action="store_true", help="Quick production setup")
    
    # Configuration options
    parser.add_argument("--base-dir", help="Base installation directory")
    parser.add_argument("--data-dir", help="Data directory")
    parser.add_argument("--config-dir", help="Configuration directory")
    parser.add_argument("--port-range", help="Port range (e.g., 9000-9999)")
    parser.add_argument("--max-memory", type=int, help="Maximum memory in MB")
    parser.add_argument("--max-disk", type=int, help="Maximum disk space in GB")
    
    # Feature flags
    parser.add_argument("--enable-auth", action="store_true", help="Enable authentication")
    parser.add_argument("--enable-ssl", action="store_true", help="Enable SSL/TLS")
    parser.add_argument("--enable-monitoring", action="store_true", help="Enable monitoring")
    parser.add_argument("--enable-backup", action="store_true", help="Enable backups")
    
    # Help
    parser.add_argument("--help-extended", action="store_true", help="Show extended help")
    
    args = parser.parse_args()
    
    # Show extended help
    if args.help_extended:
        print_help()
        return 0
    
    # Determine what to run
    async def run_command():
        try:
            if args.check_requirements:
                return await check_requirements()
            elif args.validate:
                return await validate_installation(args)
            elif args.backup:
                return await create_backup(args)
            elif args.restore:
                return await restore_backup(args.restore)
            elif args.migrate:
                return await migrate_from_single_cluster(args)
            elif args.repair:
                return await repair_installation(args)
            elif args.quick_dev:
                return await run_quick_setup(DeploymentScenario.DEVELOPMENT, args)
            elif args.quick_test:
                return await run_quick_setup(DeploymentScenario.TESTING, args)
            elif args.quick_prod:
                return await run_quick_setup(DeploymentScenario.PRODUCTION, args)
            elif args.install:
                return await run_interactive_wizard()
            else:
                # Default: run interactive wizard
                return await run_interactive_wizard()
        except KeyboardInterrupt:
            print("\n❌ Operation cancelled by user")
            return False
        except Exception as e:
            print(f"\n❌ Operation failed: {e}")
            return False
    
    # Run the command
    try:
        success = asyncio.run(run_command())
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\n❌ Operation cancelled by user")
        return 1
    except Exception as e:
        print(f"\n❌ Setup wizard failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())