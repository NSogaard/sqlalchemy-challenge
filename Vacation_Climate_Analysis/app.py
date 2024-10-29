# Import the dependencies.
# Data analysis dependencies
import numpy as np
import pandas as pd
import datetime as dt
import re

# Server / database dependencies
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, desc
from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
# Engine created to access the defined sqlite database
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Station = Base.classes.station
Measurement = Base.classes.measurement 

# Create our session (link) from Python to the DB
# The session is not defined here as keeping a session constantly open is a waste of resources. Each time a call is made
# to the app, a session is opened and closed as needed so that the session is only open when we need it to be open. To see
# the session creation / binding - see each route defined below

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

# A function that takes a string-format date as an input and outputs a list of numbers 
# representing the date (index 0 is year, index 1 is month, and index 2 is day). The function
# just does a .split() on the input string and throws the output elements through a int() 
# function to get the individual elements as integers.
def date_string_to_nums(date_string):
    return [int(val) for val in date_string.split('-')]

# This function takes in a function and will find all of the data within a date range going
# from the most recent entry to exactly one year before the most recent entry. The function
# has a optional "station" parameter to allow the user to get this data for a specific station.
# An instance of "session" is passed in so that it does need to be called here and in the calling function.
def year_from_most_recent_data(session, station=-1):
    # This variable stores the most recent date in the given dataset (it is set to an empty string now so that
    # it can be assigned to a new value in the if statement below)
    most_recent_measurement = ""

    # This is a simple query that just outputs all of the data from Measurement ordered by date (it 
    # is filtered for a specific date range when returned at the end of the function)
    year_data = session.\
        query(Measurement.station, Measurement.date, Measurement.prcp, Measurement.tobs).\
        order_by(desc(Measurement.date))

    # This determines whether or not the function will be filtered for a station - if it isn't
    # the most recent value is just assigned to 'most_recent_measurement'
    if station == -1:     
        most_recent_measurement = year_data.first()[1]
    else:
        year_data = year_data.filter(Measurement.station == station)
        most_recent_measurement = year_data.first()[1]
    
    # Date string found above is turned into a list of numbers to be converted into an instance
    # of datetime
    date_num_list = date_string_to_nums(most_recent_measurement)

    # Calculates the date exactly one year from the most recent date using datetime
    year_from_most_recent = dt.datetime(*date_num_list) - dt.timedelta(days = 365)

    # A version of the data found above filtered for the given date range is returned
    return year_data.filter(Measurement.date >= year_from_most_recent).all()

# This goes through the "Measurement" dataset and finds the station with the most measurements
# (the most active station) by using the func.count() function. 'session' is passed in so that a new
# session does not have to be opened for this function and the calling function.
def most_active_station(session):
    # The query is ran to find all of the stations in 'Measurement' by number of measurements.
    stations_by_rows = session.\
        query(Measurement.station, func.count(Measurement.date)).\
        group_by(Measurement.station).\
        order_by(desc(func.count(Measurement.date))).\
        all()

    # We output the station name / identifier (the first index of the row) of first value of the list
    # output by the query (the most active station)
    return stations_by_rows[0][0]

# This function will find the max value, min value, and mean value for a user defined data set. The end 
# parameter is optional, as the function can be called with just the start parameter to get all values from
# the given start date to the most recent value in the dataset. Session is passed in as a parameter here so 
# that we can avoid creating an instance of it here and in the calling function.
def calculate_range_metrics(session, start, end=-1):
    # This is a simple query that finds all measurements that occur after the given start date
    from_start_vals = session.\
        query(Measurement.station, Measurement.date, Measurement.prcp, Measurement.tobs).\
        filter(Measurement.date >= start)

    # If a end parameter is defined, we query again to further define the dataset to only include values that
    # were taken before the given end date
    if end != -1:
        from_start_vals = from_start_vals.filter(Measurement.date <= end)
    
    # All values are retrieved as a list using the .all() function on the query
    from_start_vals = from_start_vals.all()

    # We turn the list of values into a DataFrame so that we can call Pandas functions like .min(), .max()
    # and .mean() on the data. Null values are also dropped here
    col_names = ["station", "date", "prcp", "tobs"]
    from_start_df = pd.DataFrame(from_start_vals, columns=col_names).dropna()

    # This is a dictionary that is output by the function below. It calculates the min, max, and mean values
    # for the dataset
    output_metrics = {
        'tmin' : from_start_df["tobs"].min(),
        'tavg' : from_start_df["tobs"].mean(),
        'tmax' : from_start_df["tobs"].max()
    }

    # The above defined dictionary is returned
    return output_metrics

# This is a function that validates a date passed in by the user. If a incorrectly formatted date is passed in, an exception is raised.
# This functions checks if the input date is a string, if it takes the format 'YYYY-MM-DD', if the date can actually exist (i.e. if the 
# day of the month falls in a range that actually exists), and takes leap years into account when validating the day values for February.
def validate_date(date):
    # This checks that the input value is a string
    if type(date) != str:
        raise Exception("That date was not a string - try again with a string form date.")

    # This is a date regex created using the re package. It will be used to check the format of the input string. If the input date string
    # is of the format 'XXXX-XX-XX' where 'X' is an single place integer value, no exception will be raised.
    date_regex = r"\d{4}-\d{2}-\d{2}"

    # This checks the date string against the date regex defined above. An exception will be raised if the date is not in the 'YYYY-MM-DD'
    # format.
    if not bool(re.match(date_regex, date)):
        raise Exception("That string date was not of the correct format - try again with a string in the YYYY-MM-DD format.")
    
    # This is a call to a previously defined function. It turns the given date string into a list of integer values that can be processed
    # by the datetime package.
    date_list = date_string_to_nums(date)

    # This is a dictionary that defines the number of days in each month.
    month_lengths = {
        "1" : 31,
        "2" : 28,
        "3" : 31,
        "4" : 30,
        "5" : 31,
        "6" : 30,
        "7" : 31,
        "8" : 31,
        "9" : 30,
        "10" : 31,
        "11" : 30,
        "12" : 31,
    }

    # This if statement checks whether or not the year in the date string is a leap year. If it is, the day count for February is defined as
    # 29 - otherwise the day count for February is kept at 28. We know that a year is a leap year if it matches one of the following...
    #   1) The year is divisible by four and is NOT divisible by 100
    #   2) The year is divisible by four and IS divisible by 100 AND 400 (it has to be both)
    # All years divisible by four are leap years EXCEPT when the year ends on an even 100 (1900, 2000, 2100, etc) and is not also divisible by
    # 400 (i.e. 2000, and 2024 are leap years while 1900 and 1902 are not leap years)
    if (
        date_list[0] % 4 == 0 and
            ((date_list[0] % 100 == 0 and date_list[0] % 400 == 0) or
            (not date_list[0] % 100 == 0))
    ):
        month_lengths["2"] = 29

    # If the month value is greater than 12 or less than 1, an exception is raised because the month cannot physically exist
    if date_list[1] > 12 or date_list[1] < 1:
        raise Exception("That month does not exist")
    
    # If the day exceeds the day count for the given month defined above or falls below 1, we raise an Exception because the day cannot physically exist
    if date_list[2] > month_lengths[str(date_list[1])] or date_list[2] < 1:
        raise Exception("That is not a valid day")

    # If no errors are thrown above, the list of date values is returned (this was not originally in the functionality for this function, but returning
    # this value here made the code less cluttered in its calling function so I added it for simplicity)
    return dt.datetime(*date_list) 

#################################################
# Flask Routes
#################################################
# This is a route defined for the home page of this flask app. It returns a HTML formatted string that defines a header and ordered
# list with some basic information on all of he potential routes as well as actual urls for those routes.
@app.route("/")
def root():
    return """
        <h1>Here is a list of all of the possible paths for this Flask server:</h1>
        <ol>
            <li>Last 12 months of precipitation data ('/api/v1.0/precipitation')</li>
            <li>JSON list of all of the stations ('/api/v1.0/stations')</li>
            <li>The last year of dates and temps from the most active station (/api/v1.0/tobs)</li>
            <li>The min, average, and max temperature for the defined date range (/api/v1.0/start/end)</li>
            <li>The min, average, and max temperatures from the define date to the most recent date (/api/v1.0/start)</li>
        </ol>
    """

# This defines the route that returns the precipitation values for the last 12 months. The return value is a 
# JSON that contains dates as keys and the associated precipitation values as values.
@app.route("/api/v1.0/precipitation")
def precipitation_vals():
    # A session is started to connect to the database and pull information
    session = Session(engine)

    # All of the actual query code and data processing has been done in the 'year_from_most_recent_data()' function for
    # readability and code reusability. The function returns the data that we need to output in the format of a list of
    # tuples (see the function defined above for more details on what this actually entails).
    last_year_data = year_from_most_recent_data(session)

    # The list output by the above function is transformed into a dictionary using dictionary comprehension (index 1 in 
    # the tuple defines the date, while index 2 defines the precipitation value). The 'if type(val[2]) == float' conditional
    # is added to null check the values output by the query.
    data_dict = { val[1]:val[2] for val in last_year_data if type(val[2]) == float }

    # The session is closed to avoid unnecessary use of system resources.
    session.close()

    # A JSON version of the dictionary is returned to the user.
    return jsonify(data_dict)

# This function / route simply finds all of the stations defined by the 'Station' table. The query is not abstracted as doing so doesn't
# really improve readability given the length of the query.
@app.route("/api/v1.0/stations")
def station_list():
    # A session is opened so that we can access the data in the database.
    session = Session(engine)

    # This is a query that finds all distinct (just in case) stations in the 'Station' table
    stations = session.\
        query(Station.station).\
        distinct().\
        all()
    
    # The session is closed to avoid unnecessary use of system resources.
    session.close()
    
    # We return a JSON list of the station identifiers for all of the stations in the station list (processed using a list comprehension)
    return jsonify([station[0] for station in stations])

# This function / route returns one years worth of temperature information for the most active station (from the most recent measurement
# taken at that station). The function returns a JSON that has dates as keys and temperature data as values.
@app.route("/api/v1.0/tobs")
def most_active_year_data():
    # A session is started here so that we can access the data from the database.
    session = Session(engine)

    # The most active station is found using the 'most_active_station()' function defined above (see more details on how this works in that function)
    ma_station = most_active_station(session)

    # A years worth of data is retrieved from the most recent measurement taken at the most active station using the 'year_from_most_recent_data()' function
    # defined above using the station parameter. (for more details on how this works, see the function defined above)
    ma_station_year_data = year_from_most_recent_data(session, ma_station)
    print(ma_station)

    # The data stored in ma_station_year_data (formatted as a list of tuples) is converted into a dictionary using a dictionary comprehension. (index 1 stores
    # the date, while index 3 stores the temperature value associated with that date)
    output_dict = { val[1]:val[3] for val in ma_station_year_data }

    # The session is closed to avoid unnecessary use of system resources.
    session.close()

    # We return a JSON containing all of the values stored in the dict using the 'jsonify()' function
    return jsonify(output_dict)

# This function / route will return all values starting at the given start date and ending at the most recent measurement taken in the data set. The start date
# is validated to ensure that the date is valid, and all processing of the data is handled by predefined functions. The output of this function is a JSON containing
# the min, max, and mean values of the defined data set.
@app.route("/api/v1.0/<start>")
def from_date_metrics(start):
    # This function will validate the start date is an actually existing date
    validate_date(start)

    # A session is started so that we can access the data stored in the database.
    session = Session(engine)

    # We use an already defined function 'calculate_range_metrics' to calculate the range metrics for all values starting at the start date and ending at the most
    # recent measurement taken in the dataset.
    range_output_vals = calculate_range_metrics(session, start)

    # The session is closed to avoid unnecessary use of system resources.
    session.close()

    # A JSON list of all of the min, max, and mean values is returned to the user
    return jsonify(range_output_vals)

# This function / route will find the min, max, and mean value for the data set defined bu the user-defined start and end date. The start and end date are validated,
# then used as bounds to find the given values in the given range. The output of this function is a JSON that contains the min, max, and mean values for the defined 
# data set.
@app.route("/api/v1.0/<start>/<end>")
def date_range_metrics(start, end):
    # The start and end dates defined by the user are validated to ensure correct dates and to be used in validating the date range below (to see more details on how
    # the dates are validated, see the 'validate_date' function defined above)
    start_date = validate_date(start)
    end_date = validate_date(end)

    # This code just checks that the start date occurs before the end date (i.e. that a valid date range was defined by the user)
    if (start_date > end_date):
        raise Exception("That was not a valid date range - the end date was earlier than the start date.")

    # A session is started so that we can access the data from the database
    session = Session(engine)

    # The 'calculate_range_metrics()' function is used to find the metrics for the given date range. The output is returned as a dictionary storing the min, max, and 
    # mean values for the defined date range
    range_output_vals = calculate_range_metrics(session, start, end)

    # The session is closed to avoid unnecessary use of system resources.
    session.close()

    # A JSON list of all of the min, max, and mean values is returned to the user
    return jsonify(range_output_vals)
    
# Part of flask boilerplate code
if __name__ == "__main__":
	app.run(debug=True)
