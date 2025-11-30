
from flask import Flask, request
from math import gcd

app = Flask(__name__)

def lcm(a, b):
    return abs(a * b) // gcd(a, b)

@app.route('/svbyrs_gmail_com', methods=['GET'])
def compute_lcm():
    try:
        x = int(request.args.get('x'))
        y = int(request.args.get('y'))
        if x < 0 or y < 0:
            return "NaN"
        return str(lcm(x, y))
    except (ValueError, TypeError):
        return "NaN"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
