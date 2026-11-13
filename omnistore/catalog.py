"""Demo catalog (synthetic data). Seeded into PostgreSQL, then indexed into OpenSearch."""
from __future__ import annotations

import random

CITIES = ["Bengaluru", "Mumbai", "Delhi", "Chennai", "Hyderabad", "Pune", "Kolkata"]

# (name, description, category, price in INR, emoji)
PRODUCTS: list[tuple[str, str, str, int, str]] = [
    # Electronics
    ("Bluetooth Earphones (Neckband)", "Wireless neckband earphones with deep bass and a 20-hour battery.", "Electronics", 1499, "🎧"),
    ("True Wireless Earbuds", "Compact earbuds with a charging case and touch controls.", "Electronics", 2499, "🎧"),
    ("Bluetooth Speaker (Portable)", "Water-resistant portable speaker with 12 hours of playtime.", "Electronics", 1999, "🔊"),
    ("Smartwatch Active", "Fitness smartwatch with heart-rate and sleep tracking.", "Electronics", 3499, "⌚"),
    ("Limited Edition Smartwatch", "Numbered collector edition with a steel case. Only a handful made.", "Electronics", 8999, "⌚"),
    ("Power Bank 10000 mAh", "Slim fast-charging power bank with two USB ports.", "Electronics", 1299, "🔋"),
    ("USB-C Fast Charging Cable (1 m)", "Braided cable that supports fast charging and data transfer.", "Electronics", 399, "🔌"),
    ("Wireless Mouse", "Quiet-click wireless mouse with a USB receiver.", "Electronics", 699, "🖱️"),
    ("Mechanical Keyboard", "Compact mechanical keyboard with tactile switches.", "Electronics", 3299, "⌨️"),
    ("Smartphone Back Cover", "Shock-absorbing transparent back cover.", "Electronics", 349, "📱"),
    ("LED Desk Lamp", "Dimmable desk lamp with three colour modes.", "Electronics", 899, "💡"),
    # Home & Kitchen
    ("Mixer Grinder 750 W", "Three-jar mixer grinder for wet and dry grinding.", "Home & Kitchen", 3299, "🥤"),
    ("Pressure Cooker 5 L", "Stainless steel pressure cooker with a safety valve.", "Home & Kitchen", 1899, "🍲"),
    ("Non-stick Frying Pan", "Induction-friendly non-stick pan, 26 cm.", "Home & Kitchen", 899, "🍳"),
    ("Air Fryer 4 L", "Digital air fryer with eight preset programs.", "Home & Kitchen", 5999, "🍟"),
    ("Steel Water Bottle 1 L", "Insulated bottle that keeps drinks cold for 24 hours.", "Home & Kitchen", 549, "🍶"),
    ("Cotton Bedsheet (Double)", "Soft 100% cotton double bedsheet with two pillow covers.", "Home & Kitchen", 1199, "🛏️"),
    ("Electric Kettle 1.5 L", "Auto shut-off electric kettle for tea and instant meals.", "Home & Kitchen", 999, "☕"),
    ("Storage Containers Set", "Airtight kitchen containers, set of 6.", "Home & Kitchen", 649, "🥡"),
    # Fashion
    ("Cotton Kurta", "Breathable cotton kurta for everyday wear.", "Fashion", 1299, "👘"),
    ("Running Shoes", "Lightweight running shoes with cushioned soles.", "Fashion", 2799, "👟"),
    ("Slim Fit Jeans", "Stretch denim slim fit jeans.", "Fashion", 1799, "👖"),
    ("Canvas Backpack", "Everyday backpack with a padded laptop sleeve.", "Fashion", 1499, "🎒"),
    ("Polarised Sunglasses", "UV-protected polarised sunglasses.", "Fashion", 1199, "🕶️"),
    ("Cotton T-Shirt (Pack of 2)", "Round-neck cotton t-shirts, pack of two.", "Fashion", 799, "👕"),
    ("Leather Wallet", "Slim genuine leather wallet with card slots.", "Fashion", 699, "👛"),
    # Grocery
    ("Basmati Rice 5 kg", "Aged long-grain basmati rice.", "Grocery", 749, "🍚"),
    ("Masala Tea 500 g", "Strong Assam tea blended with whole spices.", "Grocery", 299, "🍵"),
    ("Cow Ghee 1 L", "Slow-churned cow ghee with a rich aroma.", "Grocery", 699, "🧈"),
    ("Roasted Almonds 500 g", "Lightly salted roasted almonds.", "Grocery", 549, "🥜"),
    ("Filter Coffee Powder 500 g", "Coffee and chicory blend for South Indian filter coffee.", "Grocery", 449, "☕"),
    ("Extra Virgin Olive Oil 1 L", "Cold-pressed extra virgin olive oil.", "Grocery", 899, "🫒"),
    ("Millet Cookies (Pack of 6)", "Crunchy ragi and jowar cookies with no refined flour.", "Grocery", 299, "🍪"),
    # Beauty & Care
    ("Herbal Shampoo 340 ml", "Gentle herbal shampoo for daily use.", "Beauty & Care", 349, "🧴"),
    ("Sunscreen SPF 50", "Lightweight non-greasy sunscreen, 100 ml.", "Beauty & Care", 499, "🧴"),
    ("Beard Trimmer", "Cordless trimmer with 20 length settings.", "Beauty & Care", 1299, "🪒"),
    ("Face Wash 100 ml", "Oil-control face wash with neem.", "Beauty & Care", 249, "🧼"),
    ("Coconut Hair Oil 500 ml", "Pure cold-pressed coconut hair oil.", "Beauty & Care", 299, "🥥"),
    # Sports & Fitness
    ("Yoga Mat 6 mm", "Anti-slip yoga mat with a carry strap.", "Sports & Fitness", 799, "🧘"),
    ("Adjustable Dumbbell Set", "Adjustable dumbbells from 2 kg to 10 kg.", "Sports & Fitness", 3999, "🏋️"),
    ("Badminton Racket (Pair)", "Two graphite-aluminium rackets with a cover.", "Sports & Fitness", 1599, "🏸"),
    ("Cricket Bat (Kashmir Willow)", "Full-size Kashmir willow bat.", "Sports & Fitness", 1899, "🏏"),
    ("Skipping Rope", "Speed skipping rope with adjustable length.", "Sports & Fitness", 249, "🤸"),
]

# Fixed stock for the items the demo relies on.
FIXED_STOCK: dict[str, dict[str, int]] = {
    "Bluetooth Earphones (Neckband)": {"Bengaluru": 14, "Mumbai": 20, "Delhi": 9, "Chennai": 5},
    # Exactly ONE unit in Bengaluru: the "last item" used for the two-buyers race demo.
    "Limited Edition Smartwatch": {"Bengaluru": 1, "Mumbai": 6},
}

LAST_ITEM_NAME = "Limited Edition Smartwatch"
LAST_ITEM_CITY = "Bengaluru"


def stock_plan(seed: int = 42) -> dict[str, dict[str, int]]:
    """Deterministic stock per product name -> {city: units}. Every product is sold in 3 cities."""
    rng = random.Random(seed)
    plan: dict[str, dict[str, int]] = {}
    for name, *_ in PRODUCTS:
        if name in FIXED_STOCK:
            plan[name] = dict(FIXED_STOCK[name])
        else:
            cities = rng.sample(CITIES, 3)
            plan[name] = {c: rng.choice([4, 8, 15, 25, 40]) for c in cities}
    return plan


def seed(pool) -> dict[str, int]:
    """Insert the demo catalog if the products table is empty. Returns counts."""
    with pool.connection() as conn:
        existing = conn.execute("SELECT count(*) AS n FROM products").fetchone()["n"]
        if existing:
            n_inv = conn.execute("SELECT count(*) AS n FROM inventory").fetchone()["n"]
            return {"products": existing, "listings": n_inv, "seeded": 0}
        plan = stock_plan()
        listings = 0
        for name, desc, category, price, emoji in PRODUCTS:
            pid = conn.execute(
                "INSERT INTO products (name, description, category, price, emoji) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (name, desc, category, price, emoji),
            ).fetchone()["id"]
            for city, stock in plan[name].items():
                conn.execute(
                    "INSERT INTO inventory (product_id, city, stock) VALUES (%s, %s, %s)",
                    (pid, city, stock),
                )
                listings += 1
        return {"products": len(PRODUCTS), "listings": listings, "seeded": len(PRODUCTS)}
