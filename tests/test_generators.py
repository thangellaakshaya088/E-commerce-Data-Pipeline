import pytest
from data_generator.clickstream_generator import generate_event, generate_session_events
from data_generator.orders_generator import generate_order, generate_orders_batch
from data_generator.users_generator import generate_user, generate_users_batch
from data_generator.products_generator import generate_product, generate_products_batch


class TestClickstreamGenerator:
    def test_event_has_required_fields(self):
        event = generate_event()
        for field in ["event_id", "session_id", "event_type", "page_type", "timestamp"]:
            assert field in event

    def test_event_type_is_valid(self):
        valid_types = {"page_view", "click", "add_to_cart", "remove_from_cart", "purchase", "search", "wishlist_add"}
        for _ in range(50):
            event = generate_event()
            assert event["event_type"] in valid_types

    def test_device_is_valid(self):
        for _ in range(20):
            event = generate_event()
            assert event["device"] in {"desktop", "mobile", "tablet"}

    def test_session_events_share_session_id(self):
        events = generate_session_events(5)
        assert len(events) == 5
        session_ids = {e["session_id"] for e in events}
        assert len(session_ids) == 1

    def test_purchase_event_has_product_properties(self):
        found = False
        for _ in range(200):
            event = generate_event()
            if event["event_type"] == "purchase":
                assert "product_id" in event["properties"]
                assert "price" in event["properties"]
                found = True
                break
        assert found, "Should eventually generate a purchase event"


class TestOrdersGenerator:
    def test_order_has_required_fields(self):
        order = generate_order()
        for field in ["order_id", "user_id", "status", "items", "total_amt", "created_at"]:
            assert field in order

    def test_order_total_is_positive(self):
        for _ in range(20):
            order = generate_order()
            assert order["total_amt"] > 0

    def test_order_items_not_empty(self):
        for _ in range(20):
            order = generate_order()
            assert len(order["items"]) >= 1

    def test_batch_returns_correct_count(self):
        orders = generate_orders_batch(30)
        assert len(orders) == 30

    def test_valid_status_values(self):
        valid = {"pending", "confirmed", "shipped", "delivered", "cancelled", "returned"}
        for _ in range(50):
            order = generate_order()
            assert order["status"] in valid


class TestUsersGenerator:
    def test_user_has_required_fields(self):
        user = generate_user()
        for field in ["user_id", "email", "username", "first_name", "last_name", "created_at"]:
            assert field in user

    def test_email_contains_at(self):
        for _ in range(20):
            user = generate_user()
            assert "@" in user["email"]

    def test_batch_user_ids_are_sequential(self):
        users = generate_users_batch(10)
        assert len(users) == 10
        assert users[0]["user_id"] == 1
        assert users[9]["user_id"] == 10

    def test_segment_is_valid(self):
        valid = {"new", "returning", "loyal", "at_risk", "churned"}
        for _ in range(20):
            user = generate_user()
            assert user["segment"] in valid


class TestProductsGenerator:
    def test_product_has_required_fields(self):
        product = generate_product()
        for field in ["product_id", "sku", "name", "category", "price", "final_price"]:
            assert field in product

    def test_price_is_positive(self):
        for _ in range(20):
            product = generate_product()
            assert product["price"] > 0

    def test_final_price_lte_price(self):
        for _ in range(20):
            product = generate_product()
            assert product["final_price"] <= product["price"]

    def test_batch_returns_correct_count(self):
        products = generate_products_batch(50)
        assert len(products) == 50
