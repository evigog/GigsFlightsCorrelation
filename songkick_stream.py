from songkick import *
from datetime import date
import datetime
import os
import time
import stat
import csv
import os

songkick = Songkick(api_key="8lHl4On4OYjHSxtr")

targeted_artists = ['Mumford & Sons','Godsmack','The Vaccines','Bon Iver','Disturbed','Pink','Rod Stewart',
                    'Lana del Rey','Post Malone','Dave Matthews Band','Welshly Arms','Tool','Beirut',
                    'Hot Chip','Manowar','Westlife','Tiësto','Blue October','Yann Tiersen']
popular_EU_locations = ['Berlin, Germany', 'Paris, France', 'Amsterdam, Netherlands', 'Barcelona, Spain', 
                'Copenhagen, Denmark', 'Dublin, Ireland', 'Prague, Czech Republic', 'Rome, Italy', 'Budapest, Hungary']


while True:
    path = "spark_component/data/songkick_data/"
    for file in os.listdir(path):
        aux = path + file
        if((time.time() - os.stat(aux)[stat.ST_MTIME]) > 200):
            os.remove(aux)
            print("Old file removed ",aux)

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")  
    fname = "spark_component/data/songkick_data/songkick_stream-" + str(timestamp) + ".csv"

    with open(fname,'w') as file:
        #csv_out=csv.writer(file)
        #csv_out.writerow(('concert_id', 'artist', 'date', 'city'))
        concert_id = 0 
        for a in targeted_artists:
            events = songkick.events.query(artist_name=a,
                                        min_date=date(2018,8,1),
                                        max_date=date(2019,11,1)
                                        )
            #Generator object of dictionary objetcts, one per result record
            for event in events:
                #care only for events belonging to EU locations  
                city_val = event['location']['city']    
                if city_val in popular_EU_locations:
                    # extract artist, date
                    artist_list = [p['artist']['displayName'] for p in event['performance']] #json error?
                    artist_val = artist_list[0]
                    date_val = event['start']['date']
                    city_val = str(city_val).split(",")
                    aux = str(concert_id) + "," + str(artist_val) + "," + str(date_val) + "," + str(city_val[0]) + "\n"
                    file.write(aux)
                    #record = (concert_id, artist_val, date_val, city_val)
                    #csv_out.writerow(record)
                    concert_id += 1
      
    print("Events updated at ",datetime.datetime.now())
    time.sleep(30)


