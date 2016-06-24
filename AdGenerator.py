#!/usr/bin/env python

import threading
import logging
import time
import csv
import json
import io
import random

from kafka import KafkaProducer
from random import randrange

"""AdGenerator Class

This class is used to read csv or tsv file containing text to generate
as ad. It then injects this ad to Kafka for processing.

This class is thread safe so we can spawn multiple threads to control
the load generated.
"""
class AdGenerator(threading.Thread):

    daemon = True

    """ Read ads.tsv file into a list """
    def tsv(self):
        self.ads_tsv = []
        with open("ads.tsv") as tsvfile:
            self.ads_tsv = tsvfile.readlines()

    """ Read ads.csv file and convert each entry to JSON format. """
    def csv(self):
        self.ads_csv = []
        fields = "action title keywords bid ttl url".split()
        headers = dict([(v,i) for i,v in enumerate(fields)])

        with open("ads.csv") as csvfile:
            reader = csv.reader(csvfile)
                reader.next() # skip header
                for rows in reader:
                    row = dict(zip(fields, rows))

                        # convert fields to right type
                        for int_field in "ttl".split():
                            row[int_field] = int(row[int_field])

                        for float_field in "bid".split():
                            row[float_field] = float(row[float_field])

                        #row["keywords"] = row["keywords"].split(",")

                        self.ads_csv.append(row)
        print self.ads_csv


    """ Thread run function """
    def run(self):
        #self.csv()

        """ Read in the tsv file """
        self.tsv()

        """ List of kafka bootstrap servers """
        producer = KafkaProducer(bootstrap_servers=
                'ec2-52-41-44-180.us-west-2.compute.amazonaws.com:9092,
                ec2-52-26-101-162.us-west-2.compute.amazonaws.com:9092,
                ec2-52-37-93-15.us-west-2.compute.amazonaws.com:9092,
                ec2-52-38-38-54.us-west-2.compute.amazonaws.com:9092'
                )

        """ Pick a random entry from the tsv/csv list and send it to Kafka """
        while True:
            #print "Sending: "
            #ad = json.dumps(random.choice(self.ads_csv))
            ad = random.choice(self.ads_tsv)
            print ad
            producer.send('TwitterAds', ad)
            #time.sleep(1)


""" The main """
def main():
    """ We can create multiple AdGeneratod() objects to
    increase load to Kafka """
    threads = [
            AdGenerator()
            ]

    for t in threads:
        t.start()

    """ Control the amount of time to load Kafka """
    time.sleep(10)

if __name__ == "__main__":
    #logging.basicConfig(
        #format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        #level=logging.INFO
        #)
    main()
