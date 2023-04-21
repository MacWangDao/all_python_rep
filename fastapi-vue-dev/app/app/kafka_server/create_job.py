from app.kafka_server.api_twap_pov import TWAP, Order, PoV


def create_job(record, twap_limit_count, pov_limit_count, pov_limit_sub_count):
    order = Order()
    order.volume = record.get("vol")
    order.price = record.get("price")
    order.bsdir = record.get("bsdir")
    order.exchangeid = record.get("exchangeid")
    request_type = record.get("type")
    order.securityid = record.get("stockcode", "")
    limit = record.get("limit")
    if request_type == 1:
        limit_complete = False
        if limit == 1:
            limit_complete = True
        job = TWAP(order=order, order_cycle=record["period"], step_size=record["step"],
                   cancellation_time=record["cancel"], limit_complete=limit_complete)
        job.limit_count = twap_limit_count
        return job
    elif request_type == 2:
        bookkeep = False
        if limit == 1:
            bookkeep = True
        job = PoV(order=order, transaction_rate=record["period"], step_size=record["step"],
                  cancellation_time=record["cancel"], bookkeep=bookkeep)
        job.limit_count = pov_limit_count
        job.limit_sub_count = pov_limit_sub_count
        return job
