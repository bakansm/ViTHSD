import time
import sys 

# from predict import predict
from flask import Flask, request
from flask_cors import cross_origin, CORS
from functools import wraps

def unpack(response, default_code=200):
    if not isinstance(response, tuple):
        # data only
        return response, default_code, {}
    elif len(response) == 1:
        # data only as tuple
        return response[0], default_code, {}
    elif len(response) == 2:
        # data and code
        data, code = response
        return data, code, {}
    elif len(response) == 3:
        # data, code and headers
        data, code, headers = response
        return data, code or default_code, headers
    else:
        raise ValueError("Too many response values")


def enable_cors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):

        data, code, headers = unpack(func(*args, **kwargs))

        headers['Access-Control-Allow-Origin'] = '*'
        headers['Access-Control-Allow-Methods'] = '*'
        headers['Access-Control-Allow-Headers'] = '*'
        headers['Access-Control-Max-Age'] = '1728000'
        headers['Access-Control-Expose-Headers'] = 'ETag, X-TOKEN'

        return data, code, headers

    return wrapper

# Python file
if __name__ == '__main__':
  # Create a Flask app
  app = Flask(__name__)
  CORS(app)
  
  # Add a route that accepts POST requests from the React form
  @app.route('/api/', methods=['POST'])
  @enable_cors
  def api_post():
    # Get the form data from the request
    data = request.json['text-input']
    
    # Return a response to the React form
    return jsonify({'message': 'Your name has been saved'})
    
  # Run the Flask app
  app.run(debug=True)


