from flask import Flask, jsonify

app = Flask(__name__)

app.config['SECRET_KEY'] = 'shady-business'
app.config['DEBUG'] = True


@app.route('/', methods=['GET'])
def main():
    with open('index.html', 'r') as page:
        return page.read()


@app.route('/get_data', methods=['GET'])
def get_elasticsearch_data():
    return jsonify({})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
