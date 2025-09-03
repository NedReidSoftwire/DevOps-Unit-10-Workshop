from data.database import save_order, get_all_orders
from products import create_product_download
from apscheduler.schedulers.background import BackgroundScheduler
import requests
from data.order import QUEUED
import time
import random

def initialise_scheduled_jobs(app):
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=process_orders,
        args=[app],
        trigger="interval",
        seconds=app.config["SCHEDULED_JOB_INTERVAL_SECONDS"],
    )
    scheduler.start()

def process_orders(app):
    with app.app_context():
        orders = get_queue_of_orders_to_process()
        if len(orders) == 0:
            return

        order = orders[0]

        payload = {
            "product": order.product,
            "customer": order.customer,
            "date": order.date_placed_local.isoformat(),
        }

        try:
            # Use idempotency header based on order ID to avoid duplicate charges on retries
            idempotency_key = str(order.id)
            response = _post_with_retries(
                app,
                url=app.config["FINANCE_PACKAGE_URL"] + "/ProcessPayment",
                json=payload,
                headers={"Idempotency-Key": idempotency_key},
                timeout_seconds=10,
                max_attempts=5,
                base_delay_seconds=0.5,
            )
            app.logger.info("Response from endpoint: %s", response.text)
            response.raise_for_status()

            order.set_as_processed()
            save_order(order)
        except Exception as ex:
            try:
                order.set_as_failed()
                save_order(order)
            finally:
                app.logger.exception("Failed to process order %s: %s", order.id, ex)

def get_queue_of_orders_to_process():
    allOrders = get_all_orders()
    queuedOrders = filter(lambda order: order.status == QUEUED, allOrders)
    sortedQueue = sorted(queuedOrders, key= lambda order: order.date_placed)
    return list(sortedQueue)

def _is_transient_error(response_text: str) -> bool:
    """Returns True for known transient errors we should retry."""
    if not response_text:
        return False
    text = response_text.lower()
    return (
        "table is locked" in text
        or "deadlock" in text
        or "timeout" in text
    )

def _post_with_retries(app, url: str, json: dict, headers: dict | None, timeout_seconds: int,
                       max_attempts: int = 5, base_delay_seconds: float = 0.5):
    """
    POST with exponential backoff + jitter. Retries on network errors, 5xx, and known transient messages
    (e.g., "table is locked"). Includes optional idempotency headers.
    """
    attempt = 0
    last_exc = None
    while attempt < max_attempts:
        attempt += 1
        try:
            response = requests.post(url, json=json, headers=headers or {}, timeout=timeout_seconds)
            # Retry on 5xx or transient error messages
            if response.status_code >= 500 or _is_transient_error(response.text):
                raise requests.HTTPError(f"Transient server error: {response.status_code} - {response.text}")
            return response
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as ex:
            last_exc = ex
            if attempt >= max_attempts:
                break
            # Exponential backoff with jitter
            delay = (2 ** (attempt - 1)) * base_delay_seconds
            jitter = random.uniform(0, base_delay_seconds)
            sleep_for = min(10, delay + jitter)
            app.logger.warning(
                "Attempt %s/%s failed for %s: %s. Retrying in %.2fs",
                attempt, max_attempts, url, ex, sleep_for,
            )
            time.sleep(sleep_for)
        except Exception as ex:
            # Non-retriable error
            raise ex
    # Exhausted retries
    raise last_exc if last_exc else Exception("Request failed without specific exception")
