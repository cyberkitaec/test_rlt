from connection import *
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def aggregate_salary(message: dict):
    db_rlt = get_database()
    dataset = []
    labels = []
    cur_date = message['dt_from']
    sort_id = {"_id": 1}
    if message['group_type'] == 'hour':
        format_date = "%Y-%m-%dT%H:00:00"
        date_step = timedelta(hours=1)
    elif message['group_type'] == 'day':
        format_date = "%Y-%m-%dT00:00:00"
        date_step = timedelta(days=1)
    elif message['group_type'] == 'week':
        format_date = "%Y-%U-1T00:00:00"
        date_step = timedelta(weeks=1)
    else:
        format_date = "%Y-%m-01T00:00:00"
        date_step = relativedelta(months=1)

    group_id = {"$dateToString": {"format": format_date, "date": "$dt"}}

    pipeline = [
        {"$match": {"dt": {"$gte": message['dt_from'], "$lte": message['dt_upto']}}},
        {"$group": {"_id": group_id, "total_salary": {"$sum": "$value"}}},
        {"$sort": sort_id}
    ]
    query = list(db_rlt['sample_collection'].aggregate(pipeline=pipeline))
    while cur_date <= message['dt_upto']:
        FLAG = True
        labels.append(cur_date.strftime(format_date))
        cur_date += date_step
        for item in query:
            if labels[-1] == item['_id']:
                dataset.append(item['total_salary'])
                FLAG = False
                break
        if FLAG:
            dataset.append(0)

    return {"dataset": dataset, "labels": labels}
