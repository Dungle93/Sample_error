# Sample_error

def MakeApiRequest(start_date):
    '''
    This function works as follows:
    1. Generate daily list of dates from start_date
    2. For every day it makes api request to get USD to EUR exchange rate
    3. After receiving a response it saves it to file
    :param start_date: start date
    :type start_date: int
    :return None:
    '''
    # Generate daily date from start date
    print('Start parsing ...')
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.now()
    days = (end - start).days
    for diff in range(days):
        # Make API request for every date
        parsing_date = start + timedelta(diff)
        print("Parsing: https://api.exchangeratesapi.io/" + str(parsing_date.date()))
        r1 = requests.get("https://api.exchangeratesapi.io/" + str(parsing_date.date()), {'base': "USD"})

        # Extract data and save to file
        file = open('rates.txt', "a")
        print("Saving results for: https://api.exchangeratesapi.io/" + str(parsing_date.date()))
        file.write(str(parsing_date.date()) + " " + str(r1.json()['rates']["EUR"]) + "\r")
s
MakeApiRequest("2000-01-01")
