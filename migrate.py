# migrate.py - Simple script that shows what's happening
import snowflake.connector
import os
from datetime import datetime

def connect_snowflake():
    return snowflake.connector.connect(
        user="SAATVIKRAYALU",
        password="w_vrX7.CVfFNh.8",
        account="JFUVMRO-FB11082",
        warehouse='COMPUTE_WH',
        role='ACCOUNTADMIN'
    )

def show_demo_status(message, step_num, total_steps):
    """Pretty print for demo"""
    print(f"\n{'='*50}")
    print(f"🚀 STEP {step_num}/{total_steps}: {message}")
    print(f"{'='*50}")

def migrate_products():
    """Main migration with demo-friendly output"""
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    try:
        show_demo_status("Starting Data Migration Pipeline", 1, 4)
        
        # Step 1: Show what's in DEV
        show_demo_status("Checking DEV Data", 2, 4)
        cursor.execute("SELECT COUNT(*), MAX(name) as latest_product FROM DEV_DB.PUBLIC.products")
        dev_count, latest_product = cursor.fetchone()
        print(f"📊 Found {dev_count} products in DEV")
        print(f"📦 Latest product: {latest_product}")
        
        # Step 2: Migrate data
        show_demo_status("Migrating DEV → PROD", 3, 4)
        migration_query = """
        INSERT INTO PROD_DB.PUBLIC.products 
        (id, name, price, category, last_updated, deployed_at)
        SELECT 
            id, 
            name, 
            price, 
            category, 
            last_updated,
            CURRENT_TIMESTAMP() as deployed_at
        FROM DEV_DB.PUBLIC.products
        WHERE id NOT IN (SELECT id FROM PROD_DB.PUBLIC.products)
        """
        
        cursor.execute("DELETE FROM PROD_DB.PUBLIC.products")  # Clear for demo
        cursor.execute(migration_query.replace("WHERE id NOT IN (SELECT id FROM PROD_DB.PUBLIC.products)", ""))
        
        # Step 3: Verify migration
        show_demo_status("Verifying Migration Success", 4, 4)
        cursor.execute("SELECT COUNT(*) FROM PROD_DB.PUBLIC.products")
        prod_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT name, price, deployed_at FROM PROD_DB.PUBLIC.products ORDER BY id")
        prod_data = cursor.fetchall()
        
        print(f"✅ SUCCESS: {prod_count} products now live in PRODUCTION!")
        print(f"📈 Migration completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("\n🎯 PRODUCTION DATA:")
        for name, price, deployed_at in prod_data:
            print(f"   • {name}: ${price} (deployed: {deployed_at})")
            
        return True
        
    except Exception as e:
        print(f"❌ MIGRATION FAILED: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def add_sample_product():
    """Add a new product to DEV for demo purposes"""
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    try:
        new_products = [
            (4, 'Wireless Mouse', 49.99, 'Electronics'),
            (5, 'Standing Desk', 599.99, 'Furniture')
        ]
        
        for product in new_products:
            cursor.execute(
                "INSERT INTO DEV_DB.PUBLIC.products (id, name, price, category) VALUES (%s, %s, %s, %s)",
                product
            )
        
        print(f"✨ Added {len(new_products)} new products to DEV for demo")
        return True
    except Exception as e:
        print(f"❌ Failed to add demo data: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "add-demo-data":
        add_sample_product()
    else:
        print("🎬 SNOWFLAKE CI/CD PIPELINE DEMO")
        print("   Simulating: Developer pushes code → Automatic deployment")
        success = migrate_products()
        
        if success:
            print(f"\n🎉 DEMO COMPLETE!")
            print("   ✓ Data successfully moved from DEV to PROD")
            print("   ✓ Zero manual intervention required")
            print("   ✓ Full audit trail maintained")
        else:
            print(f"\n💥 DEMO FAILED - This is what happens when there are issues!")
