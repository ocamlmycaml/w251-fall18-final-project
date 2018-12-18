from flask import Flask, jsonify, render_template, request, flash
from flask_wtf import Form
from wtforms import IntegerField, SubmitField
from wtforms import validators, ValidationError

#search form
class SearchForm(Form):
   zip = IntegerField("Please input your zipcode", [validators.Required("Please enter your zipcode.")]) 
   submit = SubmitField("Send")


#flask app
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


@app.route('/search', methods = ['GET', 'POST'])
def search():
   form = SearchForm()
   
   if request.method == 'POST':
      if form.validate() == False:
         flash('All fields are required.')
         return render_template('search.html', form = form)
      else:
        #process query here with function
        #get_elasticsearch_data() or a new function that posts the elastic search data
         
        return render_template('success.html')
   elif request.method == 'GET':
      return render_template('search.html', form = form)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
