from datetime import datetime
import faker
import psycopg2
import random

fake = faker.Faker()

def generateTransaction():
    user = fake.simple_profile()
    
    return {
        
        "transactionId" : fake.uuid4(),
        "userId" : user['username'],
        "timestamp" : datetime.utcnow().timestamp(),
        "amount" : round(random.uniform(10,10000),2),
        "currency":random.choice(["USD","GBP","INR"]),
        "city":fake.city(),
        "country": fake.country(),
        "merchantName": fake.company(),
        "paymentMethod":random.choice(["credit_card","debit_card","upi","net_banking"]),
        "ipAddress": fake.ipv4(),
        "voucherCode":random.choice(["","DISCOUNT10","FLYWAY15"]),
        "affiliateId":fake.uuid4()
        
    }
def createTable(conn):
    cursor = conn.cursor()
    
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255),
            timestamp TIMESTAMP,
            amount DECIMAL,
            currency VARCHAR(255),
            city VARCHAR(255),
            country VARCHAR(255),
            merchant_name VARCHAR(255),
            payment_method VARCHAR(255),
            ip_address VARCHAR(255),
            voucher_code VARCHAR(255),
            affiliateId VARCHAR(255)
        )
        """
    )
    cursor.close()
    conn.commit()
    
if __name__ == '__main__':
    conn = psycopg2.connect(
        host='localhost',
        database='financial_db',
        user='postgres',
        password='postgres',
        port=5432
    )

    createTable(conn)

    transaction = generateTransaction()
    cur = conn.cursor()
    print(transaction)
cur.execute(
        """
        INSERT INTO transactions(transaction_id, user_id, timestamp, amount, currency, city, country, merchant_name, payment_method, 
        ip_address, affiliateId, voucher_code)
        VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (transaction["transactionId"], transaction["userId"], datetime.fromtimestamp(transaction["timestamp"]).strftime('%Y-%m-%d %H:%M:%S'),
              transaction["amount"], transaction["currency"], transaction["city"], transaction["country"],
              transaction["merchantName"], transaction["paymentMethod"], transaction["ipAddress"],
              transaction["affiliateId"], transaction["voucherCode"])
    )

cur.close()
conn.commit()