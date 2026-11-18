"""Automated mailer — Lambda function
Sends automated emails & in-app notifications for Beat22.com

Jobs:
  1. Cart abandonment emails (every 3h)
  2. Subscription renewal reminders (every 3h, time-windowed)
  3. Inactive account re-engagement (daily, 11-13 UTC)
  4. Negotiation 24h window reminders (every 6h, time-windowed)
  5. License renewal reminders (every 3h, time-windowed)
  6. Weekly seller performance reports (weekly, Friday 13-16 UTC / ~7-10 PM IST)
  7. Seller upload nudges (every 3h, 48h upload-gap enforced internally)
  8. Seller account setup nudges (day 1 / day 3, plus a Saturday 9-12 UTC sweep)
  9. Ad campaigns stalled on an empty wallet (every 3h)
 10. Daily ad performance notification (daily, 14-16 UTC)

SCHEDULE: these gates assume an EventBridge rule of every 3 hours aligned to
00:00 UTC. Jobs 6 and 10 only ever match the 15:00 UTC run; on a 6-hourly
schedule neither would fire at all.
"""
from pymongo import MongoClient
from pymongo.database import Database
from datetime import datetime, timedelta, timezone
import os
import traceback
from jinja2 import Environment, FileSystemLoader, select_autoescape
from collections import defaultdict


import boto3
from botocore.exceptions import ClientError
import urllib.request
import urllib.parse
import json


# ---------------------------------------------------------------------------
# Module-level singletons (reused across warm Lambda invocations)
# ---------------------------------------------------------------------------
# autoescape: beat titles and display names are user-supplied and render into email HTML
jinja_env = Environment(
    loader=FileSystemLoader("templates/"),
    autoescape=select_autoescape(["html", "htm", "xml"]),
)

_mongo_client = None
_ses_client = None

def get_ses_client():
    global _ses_client
    if _ses_client is None:
        _ses_client = boto3.client('ses', region_name='ap-south-1')
    return _ses_client

def get_db():
    """Lazy-init MongoClient so warm Lambda containers reuse the connection pool."""
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient(
            os.environ["DATABASE_URL"],
            serverSelectionTimeoutMS=5000
        )
    return _mongo_client[os.environ["DATABASE_NAME"]]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

SENDER_MARKETING = "info@beat22.com"
SENDER_REPORTS = "formusicians@beat22.com"


def sendMail(to: str, subject: str, content_html: str, sender: str = SENDER_MARKETING) -> int:
    client = get_ses_client()
    try:
        response = client.send_email(
            Destination={'ToAddresses': [to]},
            Message={
                'Body': {'Html': {'Charset': "UTF-8", 'Data': content_html}},
                'Subject': {'Charset': "UTF-8", 'Data': subject},
            },
            Source=f"Beat22 <{sender}>",
        )
        return 202
    except ClientError as e:
        print("Trouble sending email via SES!")
        print(e.response['Error']['Message'])
        return 500

def mask_email(email: str) -> str:
    """Masks an email address for safe logging (PII protection)."""
    local, sep, domain = email.partition("@")
    if not sep:
        return f"{email[0]}***"
    return f"{local[0]}***@{domain}"


def safe_send_mail(to: str, subject: str, content_html: str, context: str = "", sender: str = SENDER_MARKETING) -> bool:
    """Wraps sendMail with error handling so one failed email doesn't crash the job.
    Returns True if the email was sent successfully, False otherwise."""
    try:
        status = sendMail(to, subject, content_html, sender)
        if status != 202:
            print(f"  ✗ Email failed for {mask_email(to)} ({context}): status {status}")
            return False
        print(f"  ✓ Email to {mask_email(to)} ({context}) — status: {status}")
        return True
    except Exception as e:
        print(f"  ✗ Email failed for {mask_email(to)} ({context}): {e}")
        return False


def create_notification(db: Database, user_id, notif_type: str, payload: dict, message: str):
    """Inserts a notification document directly into MongoDB.
    pymongo bypasses Mongoose middleware, so we set createdAt/updatedAt manually."""
    try:
        now = datetime.now(timezone.utc)
        db.notifications.insert_one({
            "userId": user_id,
            "type": notif_type,
            "payload": payload or {},
            "message": message,
            "unread": True,
            "createdAt": now,
            "updatedAt": now,
            "__v": 0
        })
        print(f"  ✓ Notification for {user_id} ({notif_type})")
    except Exception as e:
        print(f"  ✗ Notification failed for {user_id} ({notif_type}): {e}")

# Meta-approved UTILITY templates. Each takes one positional body var (the name)
# and has a static URL button, so no button var is ever sent.
ACC_ACTIVITY_TEMPLATES = [
    "acc_activity",
    "acc_activity_2",
    "acc_activity_4",
    "acc_activity_5",
]


def pick_acc_activity_template(user_id, now: datetime) -> str:
    """Rotates the template per user per 3-hour window so consecutive sends differ."""
    bucket = int(now.timestamp()) // (3 * 3600)
    offset = sum(str(user_id).encode())
    return ACC_ACTIVITY_TEMPLATES[(bucket + offset) % len(ACC_ACTIVITY_TEMPLATES)]


def normalize_whatsapp_number(mobile) -> str:
    """Builds an MSG91 destination number, stripping non-digits like utils/whatsapp.js does."""
    if not mobile:
        return ""

    isd = "".join(ch for ch in str(mobile.get("isd") or "") if ch.isdigit())
    number = "".join(ch for ch in str(mobile.get("number") or "") if ch.isdigit())

    if not number:
        return ""

    combined = f"{isd or '91'}{number}"

    # E.164 allows 8-15 digits
    if not (8 <= len(combined) <= 15):
        print(f"  ✗ Skipping implausible WhatsApp number (length {len(combined)})")
        return ""

    return combined


def send_whatsapp(to_number: str, template_name: str, body_vars=[], button_vars=[], header_image_url=None) -> bool:
    """Sends a WhatsApp template message using MSG91."""
    auth_key = os.environ.get("MSG91_AUTH_KEY")
    sender_number = os.environ.get("MSG91_NUMBER", "15559350672")
    if not auth_key:
        print("  ✗ MSG91_AUTH_KEY not set. Skipping WhatsApp.")
        return False
        
    components = {}
    for i, val in enumerate(body_vars):
        components[f"body_{i+1}"] = {"type": "text", "value": str(val)}
    for i, val in enumerate(button_vars):
        components[f"button_{i+1}"] = {"type": "text", "subtype": "url", "value": str(val)}
    if header_image_url:
        components["header_1"] = {"type": "image", "value": header_image_url}

    payload = {
        "integrated_number": sender_number,
        "content_type": "template",
        "payload": {
            "messaging_product": "whatsapp",
            "type": "template",
            "template": {
                "name": template_name,
                "language": {"code": "en", "policy": "deterministic"},
                "to_and_components": [
                    {
                        "to": [to_number],
                        "components": components
                    }
                ]
            }
        }
    }
    
    req = urllib.request.Request(
        "https://api.msg91.com/api/v5/whatsapp/whatsapp-outbound-message/bulk/",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "authkey": auth_key,
            "Content-Type": "application/json"
        },
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            res = response.read()
            print(f"  ✓ WhatsApp to {to_number} ({template_name})")
            return True
    except Exception as e:
        print(f"  ✗ WhatsApp failed for {to_number}: {e}")
        return False

def run_job(job_name: str, job_fn, db: Database):
    """Runs a job function in a try/except so one job failing never blocks another."""
    print(f"\n{'='*50}")
    print(f"[JOB START] {job_name}")
    print(f"{'='*50}")
    try:
        job_fn(db)
        print(f"[JOB SUCCESS] {job_name}")
    except Exception as e:
        print(f"[JOB FAILED] {job_name}: {type(e).__name__}: {e}")
        traceback.print_exc()
    print(f"{'='*50}\n")


# ---------------------------------------------------------------------------
# Job 1: Cart Abandonment (existing)
# ---------------------------------------------------------------------------

def send_pending_cart_mails(db: Database):
    """Sends mail to users who made an activity in their cart in the last 3 hours.
    This logic assumes that the cron is triggered every 3rd hour."""
    # carts storage logic: One entry per beat that a user has added to cart. 
    # 1. From the carts, identify user ids who took an action between the time period listed above
    interestedCarts = db.carts.find({"active":True, 
                   "updatedAt":
                   {
                        "$gte":datetime.now(timezone.utc) - timedelta(hours=3)
                    }}, ["user"])
    interestedInUserIds = set()
    for c in interestedCarts:
        interestedInUserIds.add(c["user"])
    
    # get all entries for the users we are interested in
    carts = db.carts.find({"active": True, "user": {"$in": list(interestedInUserIds)}})
    allUserIds = set()
    allBeatIds = set()
    # dict of userid: beats arr
    userBeatsDict = defaultdict(list)
    for cart in carts:
        userId = cart["user"]
        beatId = cart["beat"]
        allUserIds.add(userId)
        allBeatIds.add(beatId)
        userBeatsDict[userId].append(beatId)

    print(f"{len(allUserIds)} users to query")
    print(f"{len(allBeatIds)} beats to query")
    
    beats = db.beats.find({"_id": {"$in":list(allBeatIds)}, "active": True, "status":"sale"}, ["_id", "serial_number", "title", "cover_picture"])
    users = db.users.find({"_id": {"$in": list(allUserIds)}, "active": {"$ne": False}}, ["_id", "email","name", "handle", "mobile"])
    # dict of beatId to beat obj
    beatsDict = {}
    for b in beats:
        beatsDict[b["_id"]] = b
    usersDict = {}
    for u in users:
        usersDict[u["_id"]] = u

    template = jinja_env.get_template("pending-cart.html")
    #generate email html
    for userId in userBeatsDict:
        userDetails = usersDict.get(userId)
        if not userDetails:
            print(f"No details found for user {userId}. Skipping")
            continue
        name = userDetails["name"] if userDetails.get("name") else "there"
        userBeats = []
        for beatId in userBeatsDict[userId]:
            details = beatsDict.get(beatId)
            if details:
                userBeats.append(details)
                  
        if len(userBeats) == 0:
            print(f"No beat details found for user {userId}. Skipping")
            continue

        #render content
        content = template.render( {
            "name": name,
            "beats": userBeats,
            "beat_count": len(userBeats)
            }
        )
        email = userDetails.get("email")
        if not email:
            print(f"No email on record for user {userId}. Skipping")
            continue

        ideas = "idea" if len(userBeats) == 1 else "ideas"
        safe_send_mail(
            email,
            f"You left {len(userBeats)} brilliant {ideas} in your cart!",
            content,
            context=f"pending-cart ({len(userBeats)} beats)"
        )
        
        # WhatsApp notification
        mobile = userDetails.get("mobile")
        if mobile and mobile.get("isd") and mobile.get("number"):
            to_number = normalize_whatsapp_number(mobile)
            if to_number:
                send_whatsapp(
                    to_number=to_number,
                    template_name=pick_acc_activity_template(userId, datetime.now(timezone.utc)),
                    body_vars=[name],
                    button_vars=[]
                )
                
    print(f"{len(userBeatsDict)} users have pending carts updated in last 3 hours")


# ---------------------------------------------------------------------------
# Job 2: Subscription Renewal Reminders
# ---------------------------------------------------------------------------

def send_subscription_renewal_reminders(db: Database):
    """Sends email + in-app notification at 3 days, 2 days, and 1 day before subscription expiry.
    Deduplication: Query next_payment_due_at in 3-hour time windows. Since the Lambda runs
    every 3 hours, each subscription is caught by exactly one Lambda run per reminder tier."""
    
    now = datetime.now(timezone.utc)
    
    # Define the reminder windows (hours from now to expiry/ended)
    reminder_windows = [
        {"label": "3 days",  "start_hours": 69, "end_hours": 72},
        {"label": "2 days",  "start_hours": 45, "end_hours": 48},
        {"label": "1 day",   "start_hours": 21, "end_hours": 24},
        {"label": "-1 day",  "start_hours": -27, "end_hours": -24}, # exactly 1 day after expiry
    ]
    
    template = jinja_env.get_template("subscription-renewal.html")
    
    total_sent = 0
    
    for window in reminder_windows:
        window_start = now + timedelta(hours=window["start_hours"])
        window_end = now + timedelta(hours=window["end_hours"])
        days_left = window["label"]
        
        print(f"  Checking subscriptions expiring in {days_left} (window: {window_start.isoformat()} to {window_end.isoformat()})")
        
        # Find subscriptions in this window
        # Duplicate-send guard: skip subscriptions already reminded for this tier
        reminder_flag = f"reminder_{days_left.replace(' ', '_').replace('-', 'minus_')}_sent_at"
        
        if days_left == "-1 day":
            subs = list(db.subscriptions.find({
                "status": {"$in": ["expired", "cancelled", "activeTillCycleEnd", "manualCancel", "manualCancelWithErrors"]},
                "$or": [
                    {"next_payment_due_at": {"$gte": window_start, "$lt": window_end}},
                    {"end_at": {"$gte": window_start, "$lt": window_end}}
                ],
                reminder_flag: {"$exists": False}
            }))
        else:
            subs = list(db.subscriptions.find({
                "status": {"$in": ["active", "activeTillCycleEnd"]},
                "next_payment_due_at": {"$gte": window_start, "$lt": window_end},
                reminder_flag: {"$exists": False}
            }))
        
        if not subs:
            print(f"    No subscriptions found for {days_left} window")
            continue
        
        print(f"    Found {len(subs)} subscriptions in {days_left} window")
        
        # Collect unique plan IDs and user IDs
        plan_ids = set()
        user_ids = set()
        for sub in subs:
            plan_ids.add(sub["razorpay_plan_id"])
            user_ids.add(sub["user"])
        
        # Bulk-fetch subscription plans
        plans = list(db.subscription_plan_v3s.find({
            "razorpayPlanId": {"$in": list(plan_ids)}
        }))
        plans_dict = {}
        for p in plans:
            plans_dict[p["razorpayPlanId"]] = p
        
        # Bulk-fetch users (filter out inactive users — pymongo bypasses Mongoose pre-find hooks)
        users = list(db.users.find({
            "_id": {"$in": list(user_ids)},
            "active": {"$ne": False}
        }, ["_id", "email", "name", "mobile"]))
        users_dict = {}
        for u in users:
            users_dict[u["_id"]] = u
        
        # Send email + notification per subscription
        for sub in subs:
            user = users_dict.get(sub["user"])
            if not user:
                print(f"    No active user found for {sub['user']}. Skipping")
                continue
            
            email = user.get("email")
            if not email:
                print(f"    No email for user {sub['user']}. Skipping")
                continue
            
            name = user.get("name") or ""
            plan = plans_dict.get(sub["razorpay_plan_id"])
            plan_name = plan["planName"] if plan else "plan"
            frequency = sub.get("frequency", "")
            expiry_dt = sub.get("next_payment_due_at")
            expiry_date = expiry_dt.strftime("%B %d, %Y") if expiry_dt else "N/A"
            
            if days_left == "-1 day":
                # Only send WhatsApp for day-after
                to_number = normalize_whatsapp_number(user.get("mobile"))
                dt_obj = sub.get("end_at") or sub.get("next_payment_due_at")
                expiry_date_wa = dt_obj.strftime("%d/%m/%Y") if dt_obj else ""
                # MSG91 rejects empty component values
                if to_number and expiry_date_wa:
                    send_whatsapp(
                        to_number=to_number,
                        template_name="plan_deactivated",
                        body_vars=[name, plan_name, expiry_date_wa],
                        button_vars=[]
                    )
                # Mark as sent
                db.subscriptions.update_one(
                    {"_id": sub["_id"]},
                    {"$set": {reminder_flag: now}}
                )
                total_sent += 1
                continue

            # Render email
            content = template.render({
                "name": name,
                "plan_name": plan_name,
                "days_left": days_left,
                "expiry_date": expiry_date,
                "frequency": frequency,
                "renewal_url": "https://beat22.com/subscriptions"
            })
            
            # Send email
            sent = safe_send_mail(
                email,
                f"⏰ {days_left} left — Your {plan_name} is expiring soon!",
                content,
                context=f"subscription-renewal ({days_left})"
            )
            
            if sent:
                # Mark this reminder tier as sent (duplicate-send protection)
                db.subscriptions.update_one(
                    {"_id": sub["_id"]},
                    {"$set": {reminder_flag: now}}
                )
            
            # Create in-app notification
            create_notification(db, sub["user"], "SUBSCRIPTION_RENEWAL_REMINDER", {
                "planName": plan_name,
                "daysLeft": days_left,
                "expiryDate": expiry_date
            }, f"Your {plan_name} expires in {days_left}. Renew now to keep your benefits!")
            
            total_sent += 1
    
    print(f"  Total subscription renewal reminders sent: {total_sent}")


# ---------------------------------------------------------------------------
# Job 3: Inactive Account Re-engagement
# ---------------------------------------------------------------------------

def send_inactive_reengagement_emails(db: Database):
    """Sends email + notification to users whose last activity was exactly 7 days ago.
    Runs only once per day (gated to 11-13 UTC window, ~5:30 PM IST)."""
    
    now = datetime.now(timezone.utc)
    
    # Schedule gate: run only in the 11-13 UTC window (~5:30 PM IST)
    if not (11 <= now.hour <= 13):
        print("  Skipping — only runs in 11-13 UTC window (~5:30 PM IST)")
        return
    
    # Time boundaries
    seven_days_ago = now - timedelta(days=7)
    eight_days_ago = now - timedelta(days=8)
    
    print(f"  Looking for users with last activity between {eight_days_ago.isoformat()} and {seven_days_ago.isoformat()}")
    
    # Step 1: Find user IDs with activity 7-8 days ago (potentially inactive)
    potentially_inactive = set()
    
    # 1a. beat_play_analytics — uses playedAt (aliased from createdAt)
    play_cursor = db.beat_play_analytics.find({
        "playedAt": {"$gte": eight_days_ago, "$lt": seven_days_ago},
        "played_anonymously": {"$ne": True}
    }, ["playedBy_userId"])
    for doc in play_cursor:
        if doc.get("playedBy_userId"):
            potentially_inactive.add(doc["playedBy_userId"])
    
    # 1b. beat_download_analytics — uses downloadedAt (aliased from createdAt)
    download_cursor = db.beat_download_analytics.find({
        "downloadedAt": {"$gte": eight_days_ago, "$lt": seven_days_ago}
    }, ["downloadedBy_userId"])
    for doc in download_cursor:
        if doc.get("downloadedBy_userId"):
            potentially_inactive.add(doc["downloadedBy_userId"])
    
    # 1c. carts — uses updatedAt
    cart_cursor = db.carts.find({
        "updatedAt": {"$gte": eight_days_ago, "$lt": seven_days_ago}
    }, ["user"])
    for doc in cart_cursor:
        if doc.get("user"):
            potentially_inactive.add(doc["user"])
    
    print(f"  Found {len(potentially_inactive)} potentially inactive users")
    
    if not potentially_inactive:
        print("  No potentially inactive users found. Done.")
        return
    
    # Step 2: Exclude users who had activity in the last 7 days
    # Paginate in batches of 1000 if the set is large
    potentially_inactive_list = list(potentially_inactive)
    recently_active = set()
    batch_size = 1000
    
    for i in range(0, len(potentially_inactive_list), batch_size):
        batch = potentially_inactive_list[i:i + batch_size]
        
        # 2a. Recent plays
        recent_plays = db.beat_play_analytics.find({
            "playedBy_userId": {"$in": batch},
            "playedAt": {"$gte": seven_days_ago},
            "played_anonymously": {"$ne": True}
        }, ["playedBy_userId"])
        for doc in recent_plays:
            if doc.get("playedBy_userId"):
                recently_active.add(doc["playedBy_userId"])
        
        # 2b. Recent downloads
        recent_downloads = db.beat_download_analytics.find({
            "downloadedBy_userId": {"$in": batch},
            "downloadedAt": {"$gte": seven_days_ago}
        }, ["downloadedBy_userId"])
        for doc in recent_downloads:
            if doc.get("downloadedBy_userId"):
                recently_active.add(doc["downloadedBy_userId"])
        
        # 2c. Recent cart activity
        recent_carts = db.carts.find({
            "user": {"$in": batch},
            "updatedAt": {"$gte": seven_days_ago}
        }, ["user"])
        for doc in recent_carts:
            if doc.get("user"):
                recently_active.add(doc["user"])
    
    # Step 3: truly_inactive = potentially_inactive - recently_active
    truly_inactive = potentially_inactive - recently_active
    print(f"  Recently active (excluded): {len(recently_active)}")
    print(f"  Truly inactive: {len(truly_inactive)}")
    
    if not truly_inactive:
        print("  No truly inactive users. Done.")
        return
    
    # Step 4: Fetch user details (filter out inactive users and users without email)
    # Duplicate-send guard: skip users sent a re-engagement email within the last 30 days
    users = list(db.users.find({
        "_id": {"$in": list(truly_inactive)},
        "active": {"$ne": False},
        "$or": [
            {"reengagement_sent_at": {"$exists": False}},
            {"reengagement_sent_at": {"$lt": now - timedelta(days=30)}}
        ]
    }, ["_id", "email", "name"]))
    
    template = jinja_env.get_template("reengagement.html")
    
    sent_count = 0
    for user in users:
        email = user.get("email")
        if not email:
            continue
        
        name = user.get("name") or ""
        
        content = template.render({
            "name": name,
            "browse_url": "https://beat22.com"
        })
        
        sent = safe_send_mail(
            email,
            "We miss you! New beats are waiting 🎵",
            content,
            context="reengagement"
        )
        
        if sent:
            # Mark user as sent (duplicate-send protection)
            db.users.update_one(
                {"_id": user["_id"]},
                {"$set": {"reengagement_sent_at": now}}
            )
        
        create_notification(db, user["_id"], "INACTIVE_REENGAGEMENT",
            {},
            "It's been a while! Come explore the latest beats and sound kits on Beat22."
        )
        
        sent_count += 1
    
    print(f"  Total re-engagement emails sent: {sent_count}")


# ---------------------------------------------------------------------------
# Job 4: Negotiation 24h Window Reminders
# ---------------------------------------------------------------------------

def send_negotiation_purchase_reminders(db: Database):
    """Sends email + notification reminding the buyer to complete their negotiated purchase.
    Runs every 6 hours (gated). Sends at ~18h, ~12h, and ~6h before the 24h window expires."""
    
    now = datetime.now(timezone.utc)
    
    # Schedule gate: run every 6 hours (0, 6, 12, 18 UTC)
    if now.hour not in {0, 1, 6, 7, 12, 13, 18, 19}:
        print("  Skipping — only runs every 6 hours (0, 6, 12, 18 UTC)")
        return
    
    # Define reminder windows (hours from now to expiryDate)
    reminder_windows = [
        {"label": "~18 hours", "start_hours": 15, "end_hours": 21},
        {"label": "~12 hours", "start_hours": 9,  "end_hours": 15},
        {"label": "~6 hours",  "start_hours": 3,  "end_hours": 9},
    ]
    
    template = jinja_env.get_template("negotiation-reminder.html")
    
    total_sent = 0
    
    for window in reminder_windows:
        window_start = now + timedelta(hours=window["start_hours"])
        window_end = now + timedelta(hours=window["end_hours"])
        hours_remaining = window["label"]
        
        print(f"  Checking negotiations with {hours_remaining} remaining (expiry window: {window_start.isoformat()} to {window_end.isoformat()})")
        
        # Find active accepted negotiations with expiryDate in this window
        # Duplicate-send guard: skip negotiations already reminded for this tier
        reminder_flag = f"reminder_{hours_remaining.replace('~', '').replace(' ', '_')}_sent_at"
        negotiations = list(db.negotiations.find({
            "isActive": True,
            "negotiationSuccessful": {"$ne": True},
            "acceptedOffer.expiryDate": {"$gte": window_start, "$lt": window_end},
            reminder_flag: {"$exists": False}
        }))
        
        if not negotiations:
            print(f"    No negotiations found for {hours_remaining} window")
            continue
        
        print(f"    Found {len(negotiations)} negotiations in {hours_remaining} window")
        
        # Collect buyer user IDs and beat IDs
        buyer_ids = set()
        beat_ids = set()
        for neg in negotiations:
            buyer_ids.add(neg["startedByUserId"])
            beat_ids.add(neg["beatId"])
        
        # Bulk-fetch users (filter out inactive — pymongo bypasses Mongoose pre-find hooks)
        users = list(db.users.find({
            "_id": {"$in": list(buyer_ids)},
            "active": {"$ne": False}
        }, ["_id", "email", "name"]))
        users_dict = {u["_id"]: u for u in users}
        
        # Bulk-fetch beats (filter out inactive — pymongo bypasses Mongoose pre-find hooks)
        beats = list(db.beats.find({
            "_id": {"$in": list(beat_ids)},
            "active": {"$ne": False}
        }, ["_id", "title", "cover_picture", "serial_number"]))
        beats_dict = {b["_id"]: b for b in beats}
        
        # Send email + notification per negotiation
        for neg in negotiations:
            buyer = users_dict.get(neg["startedByUserId"])
            if not buyer:
                print(f"    No active user found for buyer {neg['startedByUserId']}. Skipping")
                continue
            
            email = buyer.get("email")
            if not email:
                print(f"    No email for buyer {neg['startedByUserId']}. Skipping")
                continue
            
            beat = beats_dict.get(neg["beatId"])
            beat_title = beat["title"] if beat else "your negotiated beat"
            beat_cover = beat.get("cover_picture", "") if beat else ""
            beat_serial = beat.get("serial_number", "") if beat else ""
            
            name = buyer.get("name") or ""
            price = neg.get("acceptedOffer", {}).get("price", "")
            
            # Render email
            content = template.render({
                "name": name,
                "beat_title": beat_title,
                "beat_cover": beat_cover,
                "price": price,
                "hours_remaining": hours_remaining,
                "purchase_url": "https://beat22.com/cart"
            })
            
            # Send email
            sent = safe_send_mail(
                email,
                f"⏰ {hours_remaining} left to purchase \"{beat_title}\"!",
                content,
                context=f"negotiation-reminder ({hours_remaining})"
            )
            
            if sent:
                # Mark this reminder tier as sent (duplicate-send protection)
                db.negotiations.update_one(
                    {"_id": neg["_id"]},
                    {"$set": {f"reminder_{hours_remaining.replace('~', '').replace(' ', '_')}_sent_at": now}}
                )
            
            # Create in-app notification — payload aligned with backend shape
            create_notification(db, neg["startedByUserId"], "NEGOTIATION_24H_WINDOW", {
                "type": "buying",
                "beat": {"title": beat_title, "cover_picture": beat_cover, "serial_number": beat_serial},
                "hoursRemaining": hours_remaining,
                "NEGOTIATION_24H_WINDOW": str(neg["_id"])
            }, f"Only {hours_remaining} left to purchase \"{beat_title}\". Complete your purchase now!")
            
            total_sent += 1
    print(f"  Total negotiation reminders sent: {total_sent}")


# ---------------------------------------------------------------------------
# Job 5: License Renewal Reminders
# ---------------------------------------------------------------------------

def send_license_renewal_reminders(db: Database):
    """Sends email + in-app notification + WhatsApp at 3 days, 2 days, and 1 day before license expiry.
    Deduplication: Query end_date in 3-hour time windows."""
    
    now = datetime.now(timezone.utc)
    
    windows = [
        {"days": 3, "label": "3 days"},
        {"days": 2, "label": "2 days"},
        {"days": 1, "label": "1 day"}
    ]
    
    template = jinja_env.get_template("license-renewal.html")
    
    total_sent = 0
    
    for w in windows:
        target_start = now + timedelta(days=w["days"])
        target_end = target_start + timedelta(hours=3)
        days_left = w["label"]
        
        licenses = list(db.beat_licences.find({
            "status": "active",
            "end_date": {"$gte": target_start, "$lt": target_end},
            f"reminder_{days_left.replace(' ', '_')}_sent_at": {"$exists": False}
        }))
        
        if not licenses:
            continue
            
        user_ids = {lic["buyer"] for lic in licenses}
        beat_ids = {lic["beat"] for lic in licenses}
            
        users = list(db.users.find({
            "_id": {"$in": list(user_ids)},
            "active": {"$ne": False}
        }, ["_id", "email", "name", "mobile"]))
        users_dict = {u["_id"]: u for u in users}
        
        beats = list(db.beats.find({
            "_id": {"$in": list(beat_ids)}
        }, ["_id", "title", "serial_number"]))
        beats_dict = {b["_id"]: b for b in beats}
        
        for lic in licenses:
            user = users_dict.get(lic["buyer"])
            beat = beats_dict.get(lic["beat"])
            
            if not user or not beat:
                continue
                
            email = user.get("email")
            if not email:
                continue
                
            name = user.get("name") or "User"
            beat_title = beat.get("title") or "Beat"
            license_type = lic.get("type", "Lease").capitalize()
            expiry_date = lic["end_date"].strftime("%B %d, %Y")
            
            content = template.render({
                "name": name,
                "license_type": license_type,
                "beat_title": beat_title,
                "days_left": days_left,
                "expiry_date": expiry_date,
                "renewal_url": f"https://beat22.com/bt-{beat['serial_number']}" if beat.get("serial_number") is not None else "https://beat22.com/orders"
            })
            
            sent = safe_send_mail(
                email,
                f"⏰ {days_left} left — Your license for '{beat_title}' is expiring!",
                content,
                context=f"license-renewal ({days_left})"
            )
            
            if sent:
                db.beat_licences.update_one(
                    {"_id": lic["_id"]},
                    {"$set": {f"reminder_{days_left.replace(' ', '_')}_sent_at": now}}
                )
            
            create_notification(db, lic["buyer"], "LICENSE_RENEWAL_REMINDER", {
                "licenseType": license_type,
                "beatTitle": beat_title,
                "daysLeft": days_left,
                "expiryDate": expiry_date
            }, f"Your {license_type} license for '{beat_title}' expires in {days_left}. Renew now!")
            
            if days_left == "1 day":
                to_number = normalize_whatsapp_number(user.get("mobile"))
                dt_obj = lic.get("end_date")
                expiry_date_wa = dt_obj.strftime("%d/%m/%Y") if dt_obj else ""
                # MSG91 rejects empty component values
                if to_number and expiry_date_wa:
                    send_whatsapp(
                        to_number=to_number,
                        template_name="license_renew",
                        body_vars=[name, license_type, beat_title, expiry_date_wa],
                        button_vars=[]
                    )
            
            total_sent += 1
            
    print(f"  Total license renewal reminders sent: {total_sent}")


# ---------------------------------------------------------------------------
# Job 6: Weekly Seller Performance Reports
# ---------------------------------------------------------------------------

def send_weekly_seller_reports(db: Database):
    """Sends a weekly performance summary email to each approved seller.
    Gated to run only on Fridays in the 13-16 UTC window (~7 to 10 PM IST).
    All data shown is for the past 7 days only.
    Deduplication: weekly_report_sent_at flag on the producer document."""

    now = datetime.now(timezone.utc)

    # Schedule gate: Friday only (weekday 4 = Friday), 13-16 UTC (~7-10 PM IST)
    if now.weekday() != 4:
        print("  Skipping — only runs on Fridays")
        return
    if not (13 <= now.hour <= 16):
        print("  Skipping — only runs in 13-16 UTC window")
        return

    week_ago = now - timedelta(days=7)
    two_weeks_ago = week_ago - timedelta(days=7)
    week_start_str = week_ago.strftime("%b %d")
    week_end_str = (now - timedelta(days=1)).strftime("%b %d, %Y")

    def resolve_img_url(img_path):
        if not img_path:
            return ""
        if img_path.startswith("http"):
            return img_path
        return f"https://illpeoplemusic-v2-asserts-dev.s3.ap-south-1.amazonaws.com/{img_path}"

    producers = list(db.producers.find({
        "status": "approved",
        "$or": [
            {"weekly_report_sent_at": {"$exists": False}},
            {"weekly_report_sent_at": {"$lt": week_ago}}
        ]
    }))

    if not producers:
        print("  No producers to send weekly reports to.")
        return

    print(f"  Found {len(producers)} approved producers for weekly reports")

    # ── Pre-fetch trending beat serial numbers ──────────────────────────
    trending_doc = db.trending_beats.find_one(
        {"expiresAt": {"$gte": now}},
        {"beatSerialNumbers": 1},
        sort=[("computedAt", -1)]
    )
    trending_serial_numbers = set(trending_doc.get("beatSerialNumbers", [])) if trending_doc else set()

    # ── Pre-fetch official playlists (featured) ─────────────────────────
    featured_playlists = list(db.playlists.find(
        {"isOfficialPlaylist": True, "playlistType": "beat"},
        {"beatSerialNumbers": 1, "title": 1}
    ))
    # Build a set of all serial numbers in any featured/official playlist
    featured_serial_numbers = set()
    for pl in featured_playlists:
        for sn in pl.get("beatSerialNumbers", []):
            featured_serial_numbers.add(sn)

    template = jinja_env.get_template("weekly-seller-report.html")
    sent_count = 0

    for producer in producers:
        seller_user_id = producer["user"]
        producer_id = producer["_id"]

        # ── User details ────────────────────────────────────────────────
        user = db.users.find_one({
            "_id": seller_user_id,
            "active": {"$ne": False}
        }, ["_id", "email", "name", "handle", "picture"])

        if not user or not user.get("email"):
            continue

        name = user.get("name") or user.get("handle") or ""
        handle = user.get("handle") or ""
        email = user["email"]
        profile_picture = resolve_img_url(user.get("picture"))

        # ── Seller beats ────────────────────────────────────────────────
        seller_beats = list(db.beats.find(
            {"owner": seller_user_id, "active": {"$ne": False}, "status": "sale"},
            {"_id": 1, "serial_number": 1, "title": 1, "cover_picture": 1, "genre": 1}
        ))
        seller_beat_ids = [b["_id"] for b in seller_beats]
        seller_serial_numbers = {b["serial_number"] for b in seller_beats}
        beats_by_serial = {}
        for b in seller_beats:
            beats_by_serial[b["serial_number"]] = b

        # ── Uploads ─────────────────────────────────────────────────────
        days = [(now - timedelta(days=i)).strftime("%a") for i in range(7, 0, -1)]
        daily_uploads = {d: 0 for d in days}
        
        uploads_cursor = list(db.beats.find({
            "owner": seller_user_id,
            "createdAt": {"$gte": week_ago, "$lt": now}
        }, ["createdAt"]))
        weekly_uploads = len(uploads_cursor)
        for u in uploads_cursor:
            day_str = u["createdAt"].strftime("%a")
            if day_str in daily_uploads:
                daily_uploads[day_str] += 1
                
        last_week_uploads = db.beats.count_documents({
            "owner": seller_user_id,
            "createdAt": {"$gte": two_weeks_ago, "$lt": week_ago}
        })
        uploads_change = _calc_change(weekly_uploads, last_week_uploads)

        # ── 1. Weekly Plays ─────────────────────────────────────────────
        daily_plays = {d: 0 for d in days}

        plays_cursor = db.beat_play_analytics.find({
            "owner_id": seller_user_id,
            "playedAt": {"$gte": week_ago, "$lt": now}
        })
        weekly_plays = 0
        genre_counts = {}
        for p in plays_cursor:
            weekly_plays += 1
            day_str = p["playedAt"].strftime("%a")
            if day_str in daily_plays:
                daily_plays[day_str] += 1
            
            # Count genres for distribution chart
            sn = p.get("beat_serialNumber")
            beat_info = beats_by_serial.get(sn)
            if beat_info and beat_info.get("genre"):
                genre = beat_info["genre"].capitalize()
            else:
                genre = "Other"
            genre_counts[genre] = genre_counts.get(genre, 0) + 1

        last_plays_cursor = db.beat_play_analytics.find({
            "owner_id": seller_user_id,
            "playedAt": {"$gte": two_weeks_ago, "$lt": week_ago}
        })
        last_week_plays = 0
        for p in last_plays_cursor:
            last_week_plays += 1

        plays_change = _calc_change(weekly_plays, last_week_plays)

        # ── 2. Weekly Downloads ─────────────────────────────────────────
        daily_downloads = {d: 0 for d in days}
        downloads_cursor = list(db.beat_download_analytics.find({
            "owner_id": seller_user_id,
            "downloadedAt": {"$gte": week_ago, "$lt": now}
        }, ["downloadedAt"]))
        weekly_downloads = len(downloads_cursor)
        for d in downloads_cursor:
            day_str = d["downloadedAt"].strftime("%a")
            if day_str in daily_downloads:
                daily_downloads[day_str] += 1
                
        last_week_downloads = db.beat_download_analytics.count_documents({
            "owner_id": seller_user_id,
            "downloadedAt": {"$gte": two_weeks_ago, "$lt": week_ago}
        })
        downloads_change = _calc_change(weekly_downloads, last_week_downloads)

        # ── 3. Weekly Likes ─────────────────────────────────────────────
        # user_features stores favourite_beats as an array of beat ObjectIds.
        # We count documents where any of this seller's beats appear AND
        # the document was updated this week (meaning the like happened this week).
        weekly_likes = 0
        last_week_likes = 0
        if seller_beat_ids:
            weekly_likes = db.user_features.count_documents({
                "favourite_beats": {"$in": seller_beat_ids},
                "updatedAt": {"$gte": week_ago, "$lt": now}
            })
            last_week_likes = db.user_features.count_documents({
                "favourite_beats": {"$in": seller_beat_ids},
                "updatedAt": {"$gte": two_weeks_ago, "$lt": week_ago}
            })
        likes_change = _calc_change(weekly_likes, last_week_likes)

        # ── Skip if zero data for the week ──────────────────────────────
        if weekly_plays == 0 and weekly_downloads == 0 and weekly_likes == 0:
            print(f"    Skipping {mask_email(email)} — no weekly data.")
            continue

        # ── 4. Milestones: Featured + Trending ──────────────────────────
        featured_playlist_beats = []
        for sn in seller_serial_numbers:
            if sn in featured_serial_numbers:
                b = beats_by_serial.get(sn)
                if b:
                    featured_playlist_beats.append({
                        "title": b.get("title", "Untitled"),
                        "cover": resolve_img_url(b.get("cover_picture"))
                    })

        trending_beat_list = []
        for sn in seller_serial_numbers:
            if sn in trending_serial_numbers:
                b = beats_by_serial.get(sn)
                if b:
                    trending_beat_list.append({
                        "title": b.get("title", "Untitled"),
                        "cover": resolve_img_url(b.get("cover_picture"))
                    })

        # ── 5. Top Beats This Week ──────────────────────────────────────
        top_beats_raw = list(db.beat_play_analytics.aggregate([
            {"$match": {"owner_id": seller_user_id, "playedAt": {"$gte": week_ago, "$lt": now}}},
            {"$group": {
                "_id": "$beat_serialNumber",
                "play_count": {"$sum": 1},
                "title": {"$first": "$beat_title"},
            }},
            {"$sort": {"play_count": -1}},
            {"$limit": 5}
        ]))
        top_beats = [{
            "title": b.get("title", "Untitled"),
            "play_count": b["play_count"]
        } for b in top_beats_raw]

        # ── 6. Store Overview (Weekly) ──────────────────────────────────
        active_beats = len(seller_beat_ids)

        follower_doc = db.followers.find_one({"producer_id": producer_id})
        follower_count = follower_doc.get("follower_count", 0) if follower_doc else 0

        weekly_cart_adds = db.carts.count_documents({
            "beat": {"$in": seller_beat_ids},
            "updatedAt": {"$gte": week_ago, "$lt": now}
        }) if seller_beat_ids else 0

        weekly_negotiations = db.negotiations.count_documents({
            "beatOwnerUserId": seller_user_id,
            "createdAt": {"$gte": week_ago, "$lt": now}
        })

        # ── 7. QuickChart URLs (purple palette) ────────────────────────
        # Chart 1: Comparison Line — Plays, Downloads, Uploads
        comparison_chart = {
            "type": "line",
            "data": {
                "labels": days,
                "datasets": [
                    {
                        "label": "Plays",
                        "data": [daily_plays[d] for d in days],
                        "borderColor": "#2cbb14",
                        "backgroundColor": "transparent",
                        "borderWidth": 3,
                        "pointRadius": 0
                    },
                    {
                        "label": "Downloads",
                        "data": [daily_downloads[d] for d in days],
                        "borderColor": "#ffdd00",
                        "backgroundColor": "transparent",
                        "borderWidth": 3,
                        "pointRadius": 0
                    },
                    {
                        "label": "Uploads",
                        "data": [daily_uploads[d] for d in days],
                        "borderColor": "#ef4444",
                        "backgroundColor": "transparent",
                        "borderWidth": 3,
                        "pointRadius": 0
                    }
                ]
            },
            "options": {
                "legend": {"display": False},
                "scales": {
                    "xAxes": [{"gridLines": {"display": False}, "ticks": {"fontColor": "#9ca3af", "fontSize": 10}}],
                    "yAxes": [{"gridLines": {"color": "#1a1a22"}, "ticks": {"fontColor": "#9ca3af", "fontSize": 10, "beginAtZero": True}}]
                }
            }
        }
        comparison_chart_url = f"https://quickchart.io/chart?bkg=transparent&devicePixelRatio=2&width=536&height=260&c={urllib.parse.quote(json.dumps(comparison_chart))}" if (weekly_plays > 0 or weekly_downloads > 0 or weekly_uploads > 0) else None

        # Chart 2: Doughnut — Top Genres
        sorted_genres = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)
        top_genres = sorted_genres[:4]
        other_count = sum(count for g, count in sorted_genres[4:])
        if other_count > 0:
            top_genres.append(("Other", other_count))
        
        genre_labels = [g for g, _ in top_genres]
        genre_data = [count for _, count in top_genres]
        genre_colors = ["#7C3AED", "#2cbb14", "#ffdd00", "#ef4444", "#a78bfa"]
        
        distribution_chart = {
            "type": "doughnut",
            "data": {
                "labels": genre_labels,
                "datasets": [{
                    "data": genre_data,
                    "backgroundColor": genre_colors[:len(genre_data)],
                    "borderWidth": 0
                }]
            },
            "options": {
                "legend": {"display": False},
                "plugins": {"datalabels": {"display": True, "color": "#fff", "font": {"size": 11, "weight": "bold"}}}
            }
        }
        distribution_chart_url = f"https://quickchart.io/chart?bkg=transparent&devicePixelRatio=2&width=250&height=250&c={urllib.parse.quote(json.dumps(distribution_chart))}" if genre_data else None

        # Chart 3: Horizontal Bar — Top Beats by Plays
        top_labels = []
        top_data = []
        bar_colors = ["#7C3AED", "#2cbb14", "#ffdd00"]
        for b in top_beats[:3]:
            title = b["title"]
            if len(title) > 14:
                title = title[:12] + ".."
            top_labels.append(title)
            top_data.append(b["play_count"])

        top_beats_chart = {
            "type": "horizontalBar",
            "data": {
                "labels": top_labels,
                "datasets": [{
                    "label": "Plays This Week",
                    "data": top_data,
                    "backgroundColor": bar_colors[:len(top_data)],
                    "borderWidth": 0,
                    "barPercentage": 0.8,
                    "categoryPercentage": 0.9
                }]
            },
            "options": {
                "legend": {"display": True, "position": "top", "labels": {"fontColor": "#d1d5db", "boxWidth": 12}},
                "plugins": {"datalabels": {"display": True, "align": "center", "anchor": "center", "color": "#fff", "font": {"weight": "bold"}}},
                "scales": {
                    "xAxes": [{"gridLines": {"display": False}, "ticks": {"display": False, "beginAtZero": True}}],
                    "yAxes": [{"gridLines": {"display": False}, "ticks": {"fontColor": "#d1d5db", "fontSize": 11}}]
                }
            }
        }
        top_beats_chart_url = f"https://quickchart.io/chart?bkg=transparent&devicePixelRatio=2&width=250&height=250&c={urllib.parse.quote(json.dumps(top_beats_chart))}" if top_data else None

        # ── Render & Send ───────────────────────────────────────────────
        content = template.render({
            "name": name,
            "handle": handle,
            "profile_picture": profile_picture,
            "week_start": week_start_str,
            "week_end": week_end_str,
            "weekly_uploads": weekly_uploads,
            "uploads_change": uploads_change,
            "weekly_plays": weekly_plays,
            "plays_change": plays_change,
            "weekly_downloads": weekly_downloads,
            "downloads_change": downloads_change,
            "weekly_likes": weekly_likes,
            "likes_change": likes_change,
            "featured_playlist_beats": featured_playlist_beats if featured_playlist_beats else None,
            "trending_beats": trending_beat_list if trending_beat_list else None,
            "comparison_chart_url": comparison_chart_url,
            "distribution_chart_url": distribution_chart_url,
            "top_beats_chart_url": top_beats_chart_url,
            "active_beats": active_beats,
            "follower_count": follower_count,
            "weekly_cart_adds": weekly_cart_adds,
            "weekly_negotiations": weekly_negotiations,
            "dashboard_url": "https://beat22.com/dashboard/keyPerformanceIndicators"
        })

        sent = safe_send_mail(
            email,
            f"Your Weekly Recap: {weekly_plays} plays, {weekly_downloads} downloads",
            content,
            context="weekly-seller-report",
            sender=SENDER_REPORTS
        )

        if sent:
            db.producers.update_one(
                {"_id": producer_id},
                {"$set": {"weekly_report_sent_at": now}}
            )

        # In-app notification
        create_notification(db, seller_user_id, "WEEKLY_PERFORMANCE_REPORT", {
            "weeklyPlays": weekly_plays,
            "weeklyDownloads": weekly_downloads,
            "weeklyLikes": weekly_likes
        }, "We have sent you your weekly report via email, please check that out.")

        sent_count += 1

    print(f"  Total weekly seller reports sent: {sent_count}")


def _calc_change(current: int, previous: int) -> str:
    """Returns a formatted +X% / -X% string comparing current vs previous week.
    Returns empty string when both values are zero or if there is 0% change."""
    if previous == 0 and current == 0:
        return ""
    if previous == 0:
        return "+100%"
    change = ((current - previous) / previous) * 100
    if round(change) == 0:
        return ""
    return f"+{change:.0f}%" if change > 0 else f"{change:.0f}%"


# ---------------------------------------------------------------------------
# Seller helpers
# ---------------------------------------------------------------------------

PAID_TIERS = ["tier1", "tier2"]


def get_paid_seller_ids(db: Database, user_ids) -> set:
    """User ids from the given set holding an active tier1/tier2 subscription."""
    subs = list(db.subscriptions.find(
        {"user": {"$in": list(user_ids)}, "status": {"$in": ["active", "activeTillCycleEnd"]}},
        ["user", "razorpay_plan_id"]
    ))
    if not subs:
        return set()

    plan_ids = {s.get("razorpay_plan_id") for s in subs if s.get("razorpay_plan_id")}
    paid_plan_ids = {p["razorpayPlanId"] for p in db.subscription_plan_v3s.find(
        {"razorpayPlanId": {"$in": list(plan_ids)}, "tier": {"$in": PAID_TIERS}},
        ["razorpayPlanId"]
    )}
    return {s["user"] for s in subs if s.get("razorpay_plan_id") in paid_plan_ids}


def get_sellers_with_live_campaign(db: Database, user_ids, now: datetime) -> set:
    """User ids from the given set with at least one campaign in a serving state."""
    campaigns = db.ad_campaigns.find({
        "owner": {"$in": list(user_ids)},
        "active": {"$ne": False},
        "status": "active",
        "startAt": {"$lte": now},
        "$or": [{"endAt": None}, {"endAt": {"$gte": now}}]
    }, ["owner"])
    return {c["owner"] for c in campaigns}


def get_ad_wallet_balances(db: Database, user_ids) -> dict:
    """Spendable ad wallet balance per user, credits and funded money combined."""
    wallets = db.ad_user_wallets.find({"user": {"$in": list(user_ids)}}, ["user", "balance", "creditBalance"])
    return {
        w["user"]: round(float(w.get("creditBalance") or 0) + float(w.get("balance") or 0), 2)
        for w in wallets
    }


def get_active_subscription_plans(db: Database) -> list:
    """Currently sellable paid plans, cheapest first."""
    plans = list(db.subscription_plan_v3s.find({"active": True, "tier": {"$in": PAID_TIERS}}))
    plans.sort(key=lambda p: float(p.get("price") or 0))
    return [{
        "name": p.get("planName") or "",
        "price": int(float(p.get("price") or 0)),
        "frequency": p.get("frequency") or "",
        "commission": int(float(p.get("sellerCommissionPercent") or 0)),
        "offer": p.get("offerText") or ""
    } for p in plans]


def get_pending_setup_steps(producer: dict, user: dict) -> list:
    """Setup steps a seller still has to finish before they can sell."""
    store = producer.get("store") or {}
    payment = store.get("payment") or {}
    general = store.get("general") or {}
    aadhaar = user.get("aadhaarVerification") or {}
    passport = user.get("passportValidation") or {}

    pending = []
    if not payment.get("account_id"):
        pending.append("Add your bank details")
    if not general.get("name") or not general.get("artist_first_name"):
        pending.append("Complete your store profile")
    if aadhaar.get("verified") is not True and passport.get("verified") is not True:
        pending.append("Verify your identity")
    return pending


# ---------------------------------------------------------------------------
# Job 7: Seller upload nudges
# ---------------------------------------------------------------------------

UPLOAD_NUDGE_GAP_HOURS = 48


def send_seller_upload_nudges(db: Database):
    """Prompts a seller to promote a fresh upload, but only when that upload follows a
    gap of at least 48 hours since their previous one.
    Deduplication: upload_nudge_sent_at on producers, rolling 48 hours."""

    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=3)

    recent = list(db.beats.aggregate([
        {"$match": {
            "createdAt": {"$gte": window_start},
            "active": {"$ne": False},
            "status": {"$ne": "draft"}
        }},
        {"$group": {"_id": "$owner", "latest": {"$max": "$createdAt"}, "count": {"$sum": 1}}}
    ]))

    if not recent:
        print("  No uploads in window")
        return

    qualified = {}
    for row in recent:
        previous = db.beats.find_one(
            {"owner": row["_id"], "active": {"$ne": False}, "createdAt": {"$lt": row["latest"]}},
            sort=[("createdAt", -1)], projection={"createdAt": 1}
        )
        if previous is None or (row["latest"] - previous["createdAt"]) >= timedelta(hours=UPLOAD_NUDGE_GAP_HOURS):
            qualified[row["_id"]] = row["count"]

    if not qualified:
        print("  No uploads clearing the 48 hour gap")
        return

    producers = list(db.producers.find({
        "user": {"$in": list(qualified)},
        "status": "approved",
        "$or": [
            {"upload_nudge_sent_at": {"$exists": False}},
            {"upload_nudge_sent_at": {"$lt": now - timedelta(hours=UPLOAD_NUDGE_GAP_HOURS)}}
        ]
    }, ["_id", "user"]))

    if not producers:
        print("  All qualifying sellers are within cooldown")
        return

    seller_ids = [p["user"] for p in producers]
    users = {u["_id"]: u for u in db.users.find(
        {"_id": {"$in": seller_ids}, "active": {"$ne": False}}, ["_id", "email", "name"]
    )}
    paid_ids = get_paid_seller_ids(db, seller_ids)
    campaigning_ids = get_sellers_with_live_campaign(db, seller_ids, now)
    wallets = get_ad_wallet_balances(db, seller_ids)

    ads_template = jinja_env.get_template("seller-upload-ads.html")
    upgrade_template = jinja_env.get_template("seller-upload-upgrade.html")
    active_plans = get_active_subscription_plans(db)
    total_sent = 0

    for producer in producers:
        seller_id = producer["user"]
        user = users.get(seller_id)
        if not user or not user.get("email"):
            continue

        if seller_id in campaigning_ids:
            continue

        name = user.get("name") or "there"
        beat_count = qualified.get(seller_id, 1)
        beats_label = "beat" if beat_count == 1 else "beats"

        if seller_id in paid_ids:
            has_funds = wallets.get(seller_id, 0) >= 1
            content = ads_template.render({
                "name": name,
                "beat_count": beat_count,
                "beats_label": beats_label,
                "has_funds": has_funds,
                "balance": wallets.get(seller_id, 0),
                "campaign_url": "https://beat22.com/campaign/overview"
            })
            subject = f"Put your {beats_label} in front of buyers"
            notif_type = "SELLER_UPLOAD_RUN_ADS" if has_funds else "SELLER_UPLOAD_ADD_FUNDS"
            message = (
                f"You have ₹{wallets.get(seller_id, 0)} in your ad wallet. Promote your new {beats_label}."
                if has_funds else
                f"Add funds to your ad wallet to promote your new {beats_label}."
            )
            path = "/campaign/overview"
        else:
            content = upgrade_template.render({
                "name": name,
                "beat_count": beat_count,
                "beats_label": beats_label,
                "plans": active_plans,
                "plans_url": "https://beat22.com/subscriptions"
            })
            subject = f"Sell more from your new {beats_label}"
            notif_type = "SELLER_UPLOAD_UPGRADE"
            message = f"Upgrade your plan to earn more from your new {beats_label}."
            path = "/subscriptions"

        sent = safe_send_mail(user["email"], subject, content, context="seller-upload-nudge")

        if sent:
            db.producers.update_one({"_id": producer["_id"]}, {"$set": {"upload_nudge_sent_at": now}})
            create_notification(db, seller_id, notif_type, {"beatCount": beat_count, "path": path}, message)
            total_sent += 1

    print(f"  Total seller upload nudges sent: {total_sent}")


# ---------------------------------------------------------------------------
# Job 8: Seller account setup nudges
# ---------------------------------------------------------------------------

SETUP_SWEEP_COOLDOWN_DAYS = 5
# The sweep has no natural bound, so cap a run rather than fan out across the table.
SETUP_SWEEP_MAX_PRODUCERS = 500


def send_account_setup_nudges(db: Database):
    """Day 1 and day 3 nudges for new sellers who have not finished setup, plus a
    Saturday sweep of everyone still incomplete.
    Deduplication: per-tier flags, plus a shared setup_nudge_sent_at the sweep respects."""

    now = datetime.now(timezone.utc)
    template = jinja_env.get_template("account-setup-incomplete.html")
    total_sent = 0

    tiers = [
        {"flag": "setup_nudge_d1_sent_at", "days": 1},
        {"flag": "setup_nudge_d3_sent_at", "days": 3}
    ]

    batches = []
    for tier in tiers:
        window_end = now - timedelta(days=tier["days"])
        window_start = window_end - timedelta(hours=3)
        batches.append((tier["flag"], list(db.producers.find({
            "createdAt": {"$gte": window_start, "$lt": window_end},
            "status": "approved",
            tier["flag"]: {"$exists": False}
        }))))

    if now.weekday() == 5 and 9 <= now.hour <= 12:
        batches.append((None, list(db.producers.find({
            "status": "approved",
            "$or": [
                {"setup_nudge_sent_at": {"$exists": False}},
                {"setup_nudge_sent_at": {"$lt": now - timedelta(days=SETUP_SWEEP_COOLDOWN_DAYS)}}
            ]
        }).sort("createdAt", -1).limit(SETUP_SWEEP_MAX_PRODUCERS))))

    notified = set()

    for tier_flag, producers in batches:
        producers = [p for p in producers if p["_id"] not in notified]
        if not producers:
            continue

        seller_ids = [p["user"] for p in producers]
        users = {u["_id"]: u for u in db.users.find(
            {"_id": {"$in": seller_ids}, "active": {"$ne": False}}, ["_id", "email", "name", "aadhaarVerification", "passportValidation"]
        )}

        for producer in producers:
            user = users.get(producer["user"])
            if not user or not user.get("email"):
                continue

            pending = get_pending_setup_steps(producer, user)
            if not pending:
                continue

            content = template.render({
                "name": user.get("name") or "there",
                "pending_steps": pending,
                "setup_url": "https://beat22.com/studio-settings/account"
            })

            sent = safe_send_mail(
                user["email"],
                "Finish setting up your Beat22 seller account",
                content,
                context="account-setup-incomplete"
            )

            if sent:
                notified.add(producer["_id"])
                update = {"setup_nudge_sent_at": now}
                if tier_flag:
                    update[tier_flag] = now
                db.producers.update_one({"_id": producer["_id"]}, {"$set": update})
                create_notification(db, producer["user"], "ACCOUNT_SETUP_INCOMPLETE", {
                    "pendingSteps": pending,
                    "path": "/studio-settings/account"
                }, f"{len(pending)} step(s) left before you can start selling.")
                total_sent += 1

    print(f"  Total account setup nudges sent: {total_sent}")


# ---------------------------------------------------------------------------
# Job 9: Ad campaigns stalled on an empty wallet
# ---------------------------------------------------------------------------

AD_FUNDS_COOLDOWN_HOURS = 24


def send_ad_funds_exhausted_alerts(db: Database):
    """Alerts sellers whose campaigns are still marked active but can no longer serve
    because the ad wallet cannot cover a single impression.
    Deduplication: ad_funds_alert_sent_at on producers, rolling 24 hours."""

    now = datetime.now(timezone.utc)

    campaigns = list(db.ad_campaigns.aggregate([
        {"$match": {
            "active": {"$ne": False},
            "status": "active",
            "startAt": {"$lte": now},
            "$or": [{"endAt": None}, {"endAt": {"$gte": now}}]
        }},
        {"$lookup": {"from": "ad_user_wallets", "localField": "owner", "foreignField": "user", "as": "wallet"}},
        {"$match": {"$expr": {"$and": [
            {"$lt": [{"$ifNull": [{"$arrayElemAt": ["$wallet.creditBalance", 0]}, 0]}, "$cpi"]},
            {"$lt": [{"$ifNull": [{"$arrayElemAt": ["$wallet.balance", 0]}, 0]}, "$cpi"]}
        ]}}},
        {"$group": {"_id": "$owner", "names": {"$push": "$name"}}}
    ]))

    if not campaigns:
        print("  No campaigns stalled on funds")
        return

    seller_ids = [c["_id"] for c in campaigns]
    producers = list(db.producers.find({
        "user": {"$in": seller_ids},
        "$or": [
            {"ad_funds_alert_sent_at": {"$exists": False}},
            {"ad_funds_alert_sent_at": {"$lt": now - timedelta(hours=AD_FUNDS_COOLDOWN_HOURS)}}
        ]
    }, ["_id", "user"]))

    if not producers:
        print("  All affected sellers are within cooldown")
        return

    users = {u["_id"]: u for u in db.users.find(
        {"_id": {"$in": [p["user"] for p in producers]}, "active": {"$ne": False}}, ["_id", "email", "name", "mobile"]
    )}
    names_by_owner = {c["_id"]: c["names"] for c in campaigns}
    template = jinja_env.get_template("ad-campaign-paused.html")
    total_sent = 0

    for producer in producers:
        user = users.get(producer["user"])
        if not user:
            continue

        campaign_names = names_by_owner.get(producer["user"], [])

        if user.get("email"):
            content = template.render({
                "name": user.get("name") or "there",
                "campaigns": campaign_names,
                "wallet_url": "https://beat22.com/campaign/overview"
            })

            safe_send_mail(
                user["email"],
                "Your campaigns have stopped, add funds to resume",
                content,
                context="ad-funds-exhausted"
            )

        db.producers.update_one({"_id": producer["_id"]}, {"$set": {"ad_funds_alert_sent_at": now}})
        create_notification(db, producer["user"], "AD_CAMPAIGN_PAUSED_FUNDS", {
            "campaigns": campaign_names,
            "path": "/campaign/overview"
        }, "Your ad wallet is empty and your campaigns have stopped serving. Add funds to resume.")

        mobile = user.get("mobile")
        if mobile and mobile.get("isd") and mobile.get("number"):
            to_number = normalize_whatsapp_number(mobile)
            if to_number:
                send_whatsapp(to_number=to_number, template_name="ad_campaign_paused", body_vars=[user.get("name") or "there"], button_vars=[])

        total_sent += 1

    print(f"  Total ad funds alerts sent: {total_sent}")


# ---------------------------------------------------------------------------
# Job 10: Daily ad performance notification
# ---------------------------------------------------------------------------

def send_daily_ad_metrics(db: Database):
    """In-app only: yesterday's campaign performance for sellers who served ads.
    Deduplication: ad_metrics_sent_at on producers, rolling 20 hours."""

    now = datetime.now(timezone.utc)

    if not (14 <= now.hour <= 16):
        print("  Skipping, only runs in 14-16 UTC window (~8:30 PM IST)")
        return

    period_end = now.replace(hour=0, minute=0, second=0, microsecond=0)
    period_start = period_end - timedelta(days=1)

    # Only campaigns that billed an impression in the window have metrics to report.
    owners = {c["_id"]: c["campaigns"] for c in db.ad_campaigns.aggregate([
        {"$match": {"active": {"$ne": False}, "lastImpressionAt": {"$gte": period_start}}},
        {"$group": {"_id": "$owner", "campaigns": {"$push": "$_id"}}}
    ])}

    if not owners:
        print("  No campaigns")
        return

    producers = list(db.producers.find({
        "user": {"$in": list(owners)},
        "$or": [
            {"ad_metrics_sent_at": {"$exists": False}},
            {"ad_metrics_sent_at": {"$lt": now - timedelta(hours=20)}}
        ]
    }, ["_id", "user"]))

    total_sent = 0

    for producer in producers:
        campaign_ids = owners.get(producer["user"]) or []
        if not campaign_ids:
            continue

        rows = list(db.ad_events.aggregate([
            {"$match": {
                "campaign": {"$in": campaign_ids},
                "createdAt": {"$gte": period_start, "$lt": period_end}
            }},
            {"$group": {
                "_id": "$type",
                "count": {"$sum": 1},
                "spend": {"$sum": {"$ifNull": ["$cpiAtEvent", 0]}},
                "revenue": {"$sum": {"$convert": {"input": "$meta.amount", "to": "double", "onError": 0, "onNull": 0}}}
            }}
        ]))

        stats = {r["_id"]: r for r in rows}
        impressions = stats.get("impression", {}).get("count", 0)
        if not impressions:
            continue

        clicks = stats.get("click", {}).get("count", 0)
        purchases = stats.get("purchase", {}).get("count", 0)
        spend = round(stats.get("impression", {}).get("spend", 0), 2)
        revenue = round(stats.get("purchase", {}).get("revenue", 0), 2)
        ctr = round((clicks / impressions) * 100, 1) if impressions else 0

        message = (
            f"Yesterday: {impressions} impressions, {clicks} clicks ({ctr}% CTR), "
            f"₹{spend} spent, {purchases} sale(s) worth ₹{revenue}."
        )

        create_notification(db, producer["user"], "AD_DAILY_METRICS", {
            "impressions": impressions,
            "clicks": clicks,
            "ctr": ctr,
            "spend": spend,
            "purchases": purchases,
            "revenue": revenue,
            "path": "/campaign/overview"
        }, message)

        db.producers.update_one({"_id": producer["_id"]}, {"$set": {"ad_metrics_sent_at": now}})
        total_sent += 1

    print(f"  Total daily ad metrics sent: {total_sent}")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def main():
    db = get_db()
    print("Connected to database...")

    now = datetime.now(timezone.utc)
    print(f"Lambda invoked at {now.isoformat()} UTC")

    # Job 1: Cart abandonment (runs every 3h)
    run_job("Cart Abandonment", send_pending_cart_mails, db)

    # Job 2: Subscription renewal (runs every 3h, time-windowed internally)
    run_job("Subscription Renewal", send_subscription_renewal_reminders, db)

    # Job 3: Inactive re-engagement (gated internally to run daily 00-03 UTC)
    run_job("Inactive Re-engagement", send_inactive_reengagement_emails, db)

    # Job 4: Negotiation reminders (gated internally to run every 6h)
    run_job("Negotiation Reminders", send_negotiation_purchase_reminders, db)

    # Job 5: License Renewal Reminders (Every 3h, time-windowed)
    run_job("License Renewal", send_license_renewal_reminders, db)

    # Job 6: Weekly seller performance reports (gated internally to Friday 13-16 UTC)
    run_job("Weekly Seller Report", send_weekly_seller_reports, db)

    # Job 7: Seller upload nudges (runs every 3h, 48h upload gap enforced internally)
    run_job("Seller Upload Nudges", send_seller_upload_nudges, db)

    # Job 8: Account setup nudges (day 1 / day 3, plus a Saturday 9-12 UTC sweep)
    run_job("Account Setup Nudges", send_account_setup_nudges, db)

    # Job 9: Ad campaigns stalled on an empty wallet (runs every 3h)
    run_job("Ad Funds Exhausted", send_ad_funds_exhausted_alerts, db)

    # Job 10: Daily ad performance notification (gated internally to 14-16 UTC)
    run_job("Daily Ad Metrics", send_daily_ad_metrics, db)

    print("All jobs complete.")

def lambda_handler(event = None,context = None):
    """Function handler invoked by AWS"""
    main()


# execute code if called directly
if __name__ == "__main__":
    lambda_handler()