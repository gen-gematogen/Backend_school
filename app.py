#!flask/bin/python
from flask import Flask, jsonify, abort, request, make_response
import sqlite3 as sq
from datetime import datetime

courier_types = {"foot", "bike", "car"}
lifting_capacity = {"foot": 10, "bike": 15, "car": 50}
db = "couriers.db"
MAX_WEIGHT = 50
MIN_WEIGHT = 0.01

app = Flask(__name__)

@app.route('/post/couriers', methods=['POST'])
def post_courier():
    conn = sq.connect(db)
    cursor = conn.cursor()

    try:
        data = request.get_json(force=True)
    except:
        conn.close()
        return make_response("Can't retrieve JSON", 400)

    valid = []
    invalid = []

    for courier in data["data"]:
        if (set(courier.keys()) == {"courier_id", "courier_type", "regions", "working_hours"} and
                "courier_type" in courier and "regions" in courier and "working_hours" in courier and
                (courier["courier_type"] in courier_types) and len(courier["regions"]) and len(courier["working_hours"])):
            valid.append({"id": courier["courier_id"]})
        else:
            invalid.append({"id": courier["courier_id"]})
            continue

        params = [courier['courier_id'], courier['courier_type']]
        cursor.execute("INSERT INTO couriers VALUES (?, ?)", params)
        conn.commit()

        for region in courier['regions']:
            params = [courier['courier_id'], region]
            cursor.execute("INSERT INTO regions VALUES (?, ?)", params)
            conn.commit()

        for hours in courier['working_hours']:
            pos = hours.find('-')
            params = [courier['courier_id'], hours[:pos], hours[pos + 1:]]
            cursor.execute(
                "INSERT INTO working_hours VALUES (?, ?, ?)", params)
            conn.commit()

    conn.close()

    if invalid:
        return make_response(jsonify({"validation_error": {"couriers": invalid}}), 400)

    return make_response(jsonify({"couriers": valid}), 201)


@app.route('/couriers/<int:courier_id>', methods=['POST', 'PATCH'])
def patch_courier(courier_id):
    conn = sq.connect(db)
    cursor = conn.cursor()

    try:
        data = request.get_json(force=True)
    except:
        conn.close()
        return make_response("Can't retrieve JSON", 400)

    courier_type = ""
    regions = []
    working_hours = []

    # update information about courier
    if "courier_type" in data:
        courier_type = data["courier_type"]
        req = f"""UPDATE couriers SET courier_type = '{courier_type}'
                    WHERE courier_id = {courier_id}"""
        cursor.execute(req)
        conn.commit()
    if "regions" in data:
        req = f"""DELETE FROM regions WHERE courier_id = {courier_id}"""
        cursor.execute(req)
        conn.commit()
        for region in data["regions"]:
            regions.append(region)
            req = f"""INSERT INTO regions VALUES ({courier_id}, {region})"""
            cursor.execute(req)
            conn.commit()
    if "working_hours" in data:
        req = f"""DELETE FROM working_hours WHERE courier_id = {courier_id}"""
        cursor.execute(req)
        conn.commit()
        for hours in data["working_hours"]:
            working_hours.append(hours)
            pos = hours.find('-')
            req = f"""INSERT INTO working_hours VALUES
                        ({courier_id}, '{hours[:pos]}', '{hours[pos + 1:]}')"""
            cursor.execute(req)
            conn.commit()

    # get all information about updated courier
    if len(courier_type) == 0:
        req = f"""SELECT courier_type FROM couriers
                    WHERE courier_id = {courier_id}"""
        cursor.execute(req)
        courier_type = cursor.fetchone()[0]
    if len(regions) == 0:
        req = f"""SELECT region FROM regions WHERE
                    courier_id = {courier_id}"""
        cursor.execute(req)
        regions = [i[0] for i in cursor.fetchall()]
    if len(working_hours) == 0:
        req = f"""SELECT hours_from, hours_to FROM working_hours
                    WHERE courier_id = {courier_id}"""
        cursor.execute(req)
        working_hours = [str(i[0] + '-' + i[1]) for i in cursor.fetchall()]

    # check if all courier's orders are suitable
    req = f"""SELECT order_id, weight, region FROM orders 
                WHERE assigned = {courier_id}
                AND completed = 0"""
    cursor.execute(req)
    courier_orders = cursor.fetchall()
    unavailable_order_id = []

    for order in courier_orders:
        print(order)
        # order too heavy
        if order[1] > lifting_capacity[courier_type]:
            unavailable_order_id.append(order[0])
        # order for unsuported region
        elif order[2] not in regions:
            unavailable_order_id.append(order[0])
        # order for invalid time period
        else:
            valid = False
            for hour in working_hours:
                pos = hour.find('-')
                req = f"""SELECT EXISTS (SELECT order_id FROM delivery_hours
                            WHERE hours_from <= '{hour[pos + 1:]}'
                            AND hours_to >= '{hour[:pos]}')"""
                cursor.execute(req)
                if cursor.fetchall()[0] == 1:
                    valid = True
                    break
            if not valid:
                unavailable_order_id.append(order[0])

    req = f"""UPDATE orders SET assigned = NULL, assignment_time = NULL
                WHERE order_id IN ({str(unavailable_order_id)[1:-1]})"""
    cursor.execute(req)
    conn.commit()

    conn.close()
    return make_response(jsonify({"courier_id": courier_id,
                                  "courier_type": courier_type,
                                  "regions": regions,
                                  "working_hours": working_hours}), 200)


@app.route('/orders/assign', methods=['POST'])
def assign_orders():
    conn = sq.connect(db)
    cursor = conn.cursor()

    try:
        data = request.get_json(force=True)
    except:
        conn.close()
        return make_response("Can't retrieve JSON", 400)

    # print(datetime.now().isoformat(sep='T'))

    # check if db contains such courier id
    cursor.execute("""SELECT EXISTS (SELECT courier_id FROM couriers
                    WHERE courier_id = ?)""", [data["courier_id"]])
    if not cursor.fetchone()[0]:
        return make_response("", 400)

    # get courier type by id
    cursor.execute("""SELECT courier_type FROM couriers
                        WHERE courier_id = ?""", [data["courier_id"]])
    courier_type = cursor.fetchone()[0]

    # get working hours of courier by id
    cursor.execute("""SELECT hours_from, hours_to FROM working_hours
                        WHERE courier_id = ?""", [data["courier_id"]])
    time_periods = cursor.fetchall()

    assigned_orders = set()

    # iterate through courier woking time segments
    for period in time_periods:
        # get all orders suitabel for thar courier in considered
        # segment of his working day
        req = f"""SELECT order_id FROM orders
                WHERE weight <= {lifting_capacity[courier_type]}
                AND region IN (SELECT region FROM regions
                                WHERE courier_id = {data['courier_id']})
                AND (assigned IS NULL OR assigned = {data["courier_id"]})
                AND completed = 0
                AND order_id IN (SELECT order_id FROM delivery_hours 
                                WHERE hours_from <= '{period[1]}'
                                AND hours_to >= '{period[0]}')"""
        cursor.execute(req)
        try:
            assigned_orders.update(set(cursor.fetchall()))
        except:
            continue

    # update database and assign courier's id to orders
    assigned_orders = [i[0] for i in assigned_orders]
    assignment_time = str(datetime.now().isoformat(sep='T')) + 'Z'

    if len(assigned_orders):
        req = f"""UPDATE orders SET assigned = {data["courier_id"]},
                    assignment_time = '{assignment_time}' 
                    WHERE order_id IN ({str(assigned_orders)[1:-1]})"""
        cursor.execute(req)
        conn.commit()
        ret_json = jsonify({"orders": [{"id": i} for i in assigned_orders],
                            "assign_time": assignment_time})
    else:
        ret_json = jsonify({"orders": []})

    conn.close()
    return make_response(ret_json, 200)


@app.route('/orders/complete', methods=['POST'])
def complete_orders():
    conn = sq.connect(db)
    cursor = conn.cursor()

    try:
        data = request.get_json(force=True)
    except:
        conn.close()
        return make_response("Can't retrieve JSON", 400)

    req = f"""SELECT courier_id FROM couriers
                WHERE courier_id = {data["courier_id"]}
                UNION ALL
                SELECT assigned FROM orders
                WHERE assigned = {data["courier_id"]}
                AND order_id = {data["order_id"]}"""
    cursor.execute(req)

    if len(cursor.fetchall()) < 2:
        conn.close()
        return make_response("", 400)

    req = f"""UPDATE orders SET completed = 1,
                completion_time = '{data["complete_time"]}'
                WHERE order_id = {data["order_id"]}"""
    cursor.execute(req)
    conn.commit()
    conn.close()

    return make_response(jsonify({"order_id": data["order_id"]}), 200)


@app.route('/post/orders', methods=['POST'])
def post_orders():
    conn = sq.connect(db)
    cursor = conn.cursor()

    try:
        data = request.get_json(force=True)
    except:
        conn.close()
        return make_response("Can't retrieve JSON", 400)

    valid = []
    invalid = []

    for order in data["data"]:
        if (set(order.keys()) == {"order_id", "weight", "region", "delivery_hours"} and
                order["weight"] <= MAX_WEIGHT and order["weight"] >= MIN_WEIGHT and len(order["delivery_hours"])):
            valid.append({"id": order["order_id"]})
        else:
            invalid.append({"id": order["order_id"]})
            continue

        params = [order["order_id"], order["weight"], order["region"]]
        cursor.execute(
            "INSERT INTO orders VALUES (?, ?, ?, NULL, 0, NULL, NULL)", params)
        conn.commit()

        for hours in order['delivery_hours']:
            pos = hours.find('-')
            params = [order['order_id'], hours[:pos], hours[pos + 1:]]
            cursor.execute(
                "INSERT INTO delivery_hours VALUES (?, ?, ?)", params)
            conn.commit()

    conn.close()

    if invalid:
        return make_response(jsonify({"validation_error": {"orders": invalid}}), 400)

    return make_response(jsonify({"orders": valid}), 201)


if __name__ == '__main__':
    conn = sq.connect(db)
    cursor = conn.cursor()

    # couriers init tables block

    cursor.execute("""CREATE TABLE IF NOT EXISTS couriers(
            'courier_id' INT UNSIGNED NOT NULL PRIMARY KEY,
            'courier_type' VARCHAR(10) NOT NULL)""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS regions(
            'courier_id' INT UNSIGNED NOT NULL,
            'region' INT UNSIGNED,
            PRIMARY KEY('courier_id', 'region'))""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS working_hours(
            'courier_id' INT UNSIGNED NOT NULL,
            'hours_from' VARCHAR(10),
            'hours_to' VARCHAR(10),
            PRIMARY KEY('courier_id', 'hours_from', 'hours_to'))""")

    # couriers init tables block

    cursor.execute("""CREATE TABLE IF NOT EXISTS orders(
            'order_id' INT UNSIGNED NOT NULL PRIMARY KEY,
            'weight' DOUBLE NOT NULL,
            'region' INT NOT NULL,
            'assigned' INT UNSIGNED DEFAULT NULL,
            'completed' CHAR DEFAULT 0,
            'assignment_time' VARCHAR(50),
            'completion_time' VARCHAR(50))""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS delivery_hours(
            'order_id' INT UNSIGNED NOT NULL,
            'hours_from' VARCHAR(10),
            'hours_to' VARCHAR(10),
            PRIMARY KEY('order_id', 'hours_from', 'hours_to'))""")

    conn.close()

    app.run(debug=True)  # app.run(host = "127.0.0.1", port = 5050)
