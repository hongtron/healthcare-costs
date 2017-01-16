import happybase
import json
from kafka import KafkaProducer
import constants
from pprint import pprint

from flask import Flask, request, render_template, url_for
app = Flask(__name__)


connection = happybase.Connection(host='localhost', port=9090)
# connection = happybase.Connection(host='hdp-m')
table = connection.table('state_totals')

producer = KafkaProducer(bootstrap_servers='localhost:9092')
# producer = KafkaProducer(bootstrap_servers='hdp-m.c.mpcs53013-2016.internal:6667')


def amount_to_range(amount):
    if 0 < amount < 25000:
        return "1"
    elif 2500 <= amount < 50000:
        return "2"
    elif 50000 <= amount < 75000:
        return "3"
    elif 75000 <= amount < 100000:
        return "4"
    elif 100000 <= amount < 200000:
        return "5"
    elif amount <= 200000:
        return "6"


@app.route("/run_search", methods=["POST"])
def run_search():
    drg = request.form["drg"]
    range = request.form["range"]
    search_params = {}
    search_params['drg'] = constants.DRGS[drg]
    search_params['range'] = constants.RANGES[range]
    rows = []
    for state in constants.STATES:
            rows.append(b'{}{}{}'.format(drg, range, state))

    output = []
    for row in rows:
        state_result = {}
        state_result['state'] = row[-2:]
        discharges = table.counter_get(row, b'total:statewide_discharges')
        returns = table.counter_get(row, b'total:statewide_returns')
        total_payments = table.counter_get(row, b'total:statewide_total_payments')
        covered_charges = table.counter_get(row, b'total:statewide_covered_charges')
        medicare_payments = table.counter_get(row, b'total:statewide_medicare_payments')
        agi = table.counter_get(row, b'total:statewide_agi')
        total_income = table.counter_get(row, b'total:statewide_total_income')
        if discharges != 0 and returns != 0:
            state_result['avg_total_payments'] = total_payments/discharges
            state_result['avg_covered_charges'] = covered_charges/discharges
            state_result['avg_medicare_payments'] = medicare_payments/discharges
            state_result['avg_agi'] = agi/returns
            state_result['avg_total_income'] = total_income/returns

            output.append(state_result)

    return render_template("results.html", search_params=search_params, results=output)


@app.route("/input_data")
def input_data():
    return render_template("input_data.html", states=constants.STATES, drgs=constants.DRGS)


@app.route("/submit_financial_data", methods=["POST"])
def submit_financial_data():
    state = request.form["state"]
    agi = int(request.form["agi"])
    ti = int(request.form["ti"])
    if agi < 0 or ti < 0 or agi is None or state is None or ti is None:
        return render_template("input_data.html", status="Invalid input.")
    range = amount_to_range(agi)

    message = {}
    message['state'] = state
    message['range'] = range
    message['agi'] = agi
    message['ti'] = ti
    message = json.dumps(message)

    producer.send('alih-financial-data', b'{}'.format(message))

    return render_template("input_data.html", status="Success!", states=constants.STATES, drgs=constants.DRGS)


@app.route("/submit_procedure_data", methods=["POST"])
def submit_procedure_data():
    state = request.form["state"]
    drg = request.form["drg"]
    total_payments = int(request.form["total_payments"])
    covered_charges = int(request.form["covered_charges"])
    medicare_payments = int(request.form["medicare_payments"])
    if total_payments < 0 or covered_charges < 0 or medicare_payments < 0 \
            or total_payments is None or covered_charges is None or medicare_payments is None:
        return render_template("input_data.html", status="Invalid input.")

    message = {}
    message['state'] = state
    message['drg'] = drg
    message['total_payments'] = total_payments
    message['covered_charges'] = covered_charges
    message['medicare_payments'] = medicare_payments
    message = json.dumps(message)

    producer.send('alih-procedure-data', b'{}'.format(message))

    return render_template("input_data.html", status="Success!", states=constants.STATES, drgs=constants.DRGS)


@app.route("/")
def home():
    return render_template("home.html", drgs=constants.DRGS)

if __name__ == '__main__':
        app.run(host='0.0.0.0', debug=True)
