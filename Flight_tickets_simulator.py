import numpy as np
import time
import datetime

def map_date(t,D):
    """
        Map days indexes to dates\n
        t >> current date in format: dd/mm/yy \n
        D >> the amount of days we are going to track flights\n
    """
    day,month,year = map(int,t.split("/"))
    dates = {}
    for i in range(D):
        aux = str(day) + "/" + str(month) + "/" + str(year)
        dates.update({i:aux})
        if((day==31 and (month==1 or month==3 or month==5 or month==7 or month==8 or month==10 or month==12)) or \
        (day==30 and (month==4 or month==6 or month==9 or month==11)) or \
        (day==29 and month==2 and year%4==0 and year%100==0 and year%400==0) or \
        (day==28 and month==2 and (year%4>0 or year%100>0 or year%400>0))):
            day = 1
            if month < 12:
                month += 1
            else:
                month = 1
                year += 1
        else:
            day += 1
    return dates

def map_cities(N,C):
    """
        Map cities indexes to cities names
        N >> Amount of cities
        C >> Cities names
    """
    cities = {}
    for id_city in range(N): 
        cities.update({id_city: C[id_city]})
    return cities

def rescale(a,b,minimum,maximum,x):
    """
        Rescale data\n
        a >> minimum(base) price\n
        b >> maximum(base x 2) price\n
        minimum >> min from generated values\n
        maximum >> max from generated values\n
    """
    return (((b - a)*(x - minimum)) /  (maximum - minimum)) + a

def Initialize_prices(N,D,d,cm):
    """
        Generate initial ticket prices\n
        N >> amount of destinations\n
        D >> the amount of days we are going to track flights\n
        d >> initial date of tracking\n
        cm >> cities map\n
    """
    destinations = np.ones((N,N))
    np.fill_diagonal(destinations,0)
    variance_destinations = np.arange(0.05,0.11,0.001)
    destinations[destinations == 1] = np.random.normal(0,variance_destinations[np.random.randint(0,variance_destinations.shape[0])],destinations[destinations == 1].shape[0])
    destinations = np.where(destinations < 0.1, 0, 1)
    destinations[destinations == 1].shape[0]
    ticket_prices = np.zeros((N,N,D))
    base_prices = {}
    end_prices = {}
    for i in range(N):
        for j in range(N):
            if(destinations[i,j] != 0):
                #select randomly which days will the destination is going to have available flights(only 1 flight per day)
                variance_days = np.arange(0.09,0.2,0.001)
                rand_gen = np.random.normal(0,variance_days[np.random.randint(0,variance_days.shape[0])],D)
                flight_singleDestination = np.where(rand_gen < 0.1,0,rand_gen)
                #generate flight base price randomly(per destination rather than per date)
                base = 1000 + (np.random.rand(1) * 3000)
                base_prices.update({(i,j): base})
                #min and max from random generated dat
                min_aux = np.min(flight_singleDestination[flight_singleDestination > 0])
                max_aux = np.max(flight_singleDestination)
                #rescale random generated values for this destination flights to fit its price range
                prices = np.vectorize(rescale)
                flight_singleDestination[flight_singleDestination > 0] = prices(base,base*2,min_aux,max_aux,flight_singleDestination[flight_singleDestination > 0])
                #transform output(all) - "end_prices" holds the results
                dict_prices = transform_output(D,flight_singleDestination,d,cm[i],cm[j],end_prices)
                #save prices
                ticket_prices[i,j] = flight_singleDestination
    return ticket_prices,base_prices,end_prices

def Update_prices(current_prices,base,dates_map,cities_map):
    """
        Update price of flight tickets tracked (Only some of them will be updated, not all)\n
        Selection of flights to update done randomly\n
        current_prices >> Flight ticket prices matrix to update\n 
        base >> Flights base prices(oer destination)\n
        dates_map >> iddates vs dates map\n
        cities_map >> idcities vs cities map\n
    """
    old_prices = np.copy(current_prices)
    #Increase the prices randomly
    current_prices[current_prices > 0] = current_prices[current_prices > 0] + \
        (current_prices[current_prices > 0] * np.random.rand(current_prices[current_prices > 0].shape[0])) 
    #Adjust prices that passed the limit
    while(np.asarray([current_prices[key[0],key[1],:][current_prices[key[0],key[1],:] > base[key]*2] > 0 for key,value in base.items()]).flatten().shape[0] > 0):
        for key2,value2 in base.items():
            current_prices[key2[0],key2[1]][current_prices[key2[0],key2[1]] > base[key2]*2] =  current_prices[key2[0],key2[1]][current_prices[key2[0],key2[1]] > base[key2]*2] - \
            (((base[key2]*2)-base[key2]) * np.random.rand(current_prices[key2[0],key2[1]][current_prices[key2[0],key2[1]] > base[key2]*2].shape[0]))
    
    aux = np.where(current_prices != old_prices)
    data = ""
    for i,j,k in zip(aux[0],aux[1],aux[2]):
        data += str(cities_map[i]) + "," + str(cities_map[j]) + "," + dates_map[k] + "," + str(np.round(current_prices[i,j,k])) + "\n"
    with open("data_stream.csv","w") as f:
        f.write(data)
    return current_prices

def transform_output(D,price,date,fromCity,toCity,dict_prices):
    """
        dict prices >> {key(fromCity,toCity,date), value(price)}
        D >> number of days to track
        date >> first date to track from
        price >> ticket prices
        fromCity >> id city to travel from
        toCity >> id destination city
        
    """
    k = np.column_stack((np.full(D,fromCity),np.full(D,toCity),list(map_date(date,D).values())))
    v = price
    for ki,vi in zip(k,v):
        dict_prices.update({str(ki):vi})
    return dict_prices

def send_stream(dict_prices):
    """
        Send csv formated prices in an stream
        dict_prices: dictionary containing the prices and its keys
    """
    data = ""
    for ki,vi in dict_prices.items():
        if vi > 0:
            ki = ki.split("' '")
            aux = ki[0].split("['")[1] + "," + ki[1] + "," + ki[2].split("']")[0] + "," + str((int)(np.round(vi))) + "\n"
            data += aux
    return data

def create_data(D,d):
    """
        Create data\n
        D >> number of days to track flights for\n
        d >> initial date to track from\n
    """
    with open("cities_unique.csv","r") as f:
        cities = f.read().splitlines()
    cities = np.asarray(cities)
    N_cities= cities.shape[0]
    cities_map = map_cities(N_cities,cities)
    prices, base_prices, dprices = Initialize_prices(N_cities,D,d,cities_map)
    data = send_stream(dprices)
    #Write csv file
    with open("data_stream.csv","w") as f:
        f.write(data)
    return prices,base_prices,dprices,cities_map

def update_data(current_prices,base,date,D,ctmap):
    """
        Update data to stream\n
        current_prices >> Matrix with ticket prices\n
        base >> dict with base ticket prices\n
        date >> initial date to track from\n
        D >> number of days to track flights for\n
        ctmap >> id cities vs cities map\n
    """
    mdate = map_date(date,D)
    prices = Update_prices(current_prices,base,mdate,ctmap)
    return prices

track_days = 60
init_date = "10/10/18"

prices_matrix, base_prices, prices_dict, cities_map = create_data(track_days,init_date)
while True:
    update_data(prices_matrix,base_prices,init_date,track_days,cities_map)
    print("Prices updated at ",datetime.datetime.now())
    time.sleep(5)
    




