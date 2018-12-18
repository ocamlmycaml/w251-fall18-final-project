import requests
import json

from flask import Flask, jsonify, render_template, request, flash
from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField, SubmitField
from wtforms import validators, ValidationError

from geopy.geocoders import Nominatim


geolocator = Nominatim()


ES_COLLECTION = 'pollutants_by_county'
ES_URL = 'http://169.45.85.246:9200/{collection_name}/_search?pretty'.format(
    collection_name=ES_COLLECTION
)


def get_search_results(county):
    params = {
        'query': {
            'match_phrase': {'county': county}
        },
        'sort': [{'totrisk': 'desc'}]
    }
    print(json.dumps(params))

    stuff = requests.get(ES_URL, data=json.dumps(params))
    stuff.raise_for_status()

    num_docs = stuff.json()['hits']['total']
    documents = stuff.json()['hits']['hits']

    return num_docs, documents


#search form
class SearchForm(FlaskForm):
    city = StringField("City/County", [validators.Required("Please enter your city")])
    state = StringField("State", [validators.Required("Please enter your state")])
    submit = SubmitField("Send")


#flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'shady-business'
app.config['DEBUG'] = True


@app.route('/', methods = ['GET', 'POST'])
def search():
    form = SearchForm()

    if request.method == 'POST':
        if form.validate() == False:
            flash('All fields are required.')
            return render_template('search.html', form = form)
        else:
            # get fips code
            city = form.city.data
            state = form.state.data

            ## some exception handling required if an address is not recognized by censusgeocode
            location = geolocator.geocode('{}, {}, USA'.format(city, state))
            if len(location.address.split(",")) == 4:
                county = location.address.split(",")[1].strip().lower()
            else:
                county = city.strip().lower()

            county = county[:-6].strip() if 'county' in county else county
            hits, data = get_search_results(county)

            return render_template('search.html', form=form, hits=hits, data=data)
    elif request.method == 'GET':
        return render_template('search.html', form = form, data = None)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
