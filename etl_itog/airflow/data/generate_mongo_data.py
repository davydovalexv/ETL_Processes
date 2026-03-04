import pymongo
from datetime import datetime, timedelta
import random

client = pymongo.MongoClient("mongodb://mongo:27017/")
db = client['etl_demo']

# UserSessions
sessions = []
for i in range(10):
    sessions.append({
        "session_id": f"sess_{i+1:03}",
        "user_id": f"user_{random.randint(100,130)}",
        "start_time": datetime(2024,1,10,9,0,0) + timedelta(minutes=30*i),
        "end_time": datetime(2024,1,10,9,20,0) + timedelta(minutes=30*i),
        "pages_visited": ["/home", "/products", f"/products/{random.randint(40,99)}", "/cart"],
        "device": {"mobile": bool(random.getrandbits(1)), "platform": "android"},
        "actions": ["login", "view_product", "add_to_cart", "logout"]
    })
db.UserSessions.insert_many(sessions)

# EventLogs
logs = []
for i in range(20):
    logs.append({
        "event_id": f"evt_{1000+i}",
        "timestamp": datetime(2024,1,10,9,5,20) + timedelta(seconds=15*i),
        "event_type": random.choice(["click", "purchase", "scroll", "logout"]),
        "details": { "url": f"/products/{random.randint(40,99)}"}
    })
db.EventLogs.insert_many(logs)

# SupportTickets
tickets = []
for i in range(5):
    created_at = datetime(2024,1,9,11,55,0) + timedelta(hours=i)
    updated_at = created_at + timedelta(hours=random.randint(1,3))
    tickets.append({
        "ticket_id": f"ticket_{789+i}",
        "user_id": f"user_{random.randint(100,130)}",
        "status": random.choice(["open", "closed", "pending"]),
        "issue_type": random.choice(["payment", "delivery", "technical"]),
        "messages": [
            {"sender": "user", "message": "Не могу оплатить заказ.", "timestamp": created_at},
            {"sender": "support", "message": "Пожалуйста, уточните способ оплаты.", "timestamp": updated_at}
        ],
        "created_at": created_at,
        "updated_at": updated_at
    })
db.SupportTickets.insert_many(tickets)

# UserRecommendations
recommendations = [
    {
        "user_id": f"user_{random.randint(100,130)}",
        "recommended_products": [f"prod_{random.randint(101,201)}" for _ in range(3)],
        "last_updated": datetime(2024,1,10,8,0,0) + timedelta(days=i)
    }
    for i in range(7)
]
db.UserRecommendations.insert_many(recommendations)

# ModerationQueue
reviews = []
for i in range(8):
    reviews.append({
        "review_id": f"rev_{555+i}",
        "user_id": f"user_{random.randint(100,130)}",
        "product_id": f"prod_{101+random.randint(0,50)}",
        "review_text": "Отличный товар, работает как нужно!" if i % 2 == 0 else "Плохое качество, не работает.",
        "rating": random.randint(1,5),
        "moderation_status": random.choice(["pending", "approved", "rejected"]),
        "flags": ["contains_images"] if i % 3 == 0 else [],
        "submitted_at": datetime(2024,1,8,10,20,0) + timedelta(days=i)
    })
db.ModerationQueue.insert_many(reviews)

print("MongoDB test data loaded successfully.")