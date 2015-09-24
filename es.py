#!/usr/bin/env python2

from __future__ import print_function
import logging
import pymongo
import sys
import yaml

from elasticsearch import Elasticsearch
from pymongo import MongoClient
from pymongo.cursor import CursorType
from time import time


logging.basicConfig(format="%(asctime)s - %(levelname)s : %(message)s",
                    level=logging.INFO)


def config():
    configfile = "/opt/bernie/config.yml"
    try:
        with open(configfile, 'r') as f:
            conf = yaml.load(f)
    except IOError:
        # msg = "Could not open config file: {0}"
        # logging.info(msg.format(configfile))
        sys.exit(1)
    else:
        return conf

VERSION = "v1"


def connect_mongo():
    conf = config()
    c = conf["elasticsearch"]
    db = MongoClient(c["mongohost"], c["mongoport"])
    db.admin.authenticate(
        c["mongouser"],
        c["mongopass"],
        mechanism='SCRAM-SHA-1'
    )
    return db


def prepare(entry):
    dbname, colname = entry["ns"].split(".")

    # If an insert, set rec to inserted rec

    if entry["op"] == 'i':
        rec = entry["o"]

    # If an update, set rec to retrieved rec
    elif entry["op"] == "u":
        query = {"_id": doc["o2"]["_id"]}
        rec = db[dbname][colname].find(query)[0]
    else:
        return False
    rec["timestamp"] = time()
    rec["_id"] = str(rec["_id"])
    if "parent" in rec:
        rec["parent"] = str(rec["parent"])

    return {"body": rec, "db": dbname, "col": colname}


def get_es_values(rec):
    if rec["db"] == "bernie":
        name = [rec["col"], rec["body"]["lang"], VERSION]
        rec["es_index"] = "_".join(name)
        rec["es_type"] = rec["body"]["site"].replace(".", "_")
    elif rec["db"] == "facebook":
        if rec["col"] == "token":
            return False
        name = ["fb", rec["body"]["page"], VERSION]
        rec["es_index"] = "_".join(name)
        rec["es_type"] = rec["col"] if rec["col"] != "data" else "stats"
    elif rec["db"] == "campaign":
        name = ["campaign_events", VERSION]
        rec["es_index"] = "_".join(name)
        rec["es_type"] = rec["col"]
    else:
        return False
    return rec


if __name__ == '__main__':
    db = connect_mongo()
    conf = config()["elasticsearch"]
    es = Elasticsearch(conf["host"])
    oplog = db.local.oplog.rs
    first = next(oplog.find().sort('$natural', pymongo.DESCENDING).limit(-1))
    ts = first['ts']

    while True:
        cursor = oplog.find(
            {'ts': {'$gt': ts}},
            cursor_type=CursorType.TAILABLE_AWAIT,
            oplog_replay=True
        )
        while cursor.alive:
            for doc in cursor:
                ts = doc['ts']
                rec = prepare(doc)
                if rec:
                    rec = get_es_values(rec)
                if not rec:
                    continue
                if "parent" in rec["body"]:
                    es.index(
                        index=rec["es_index"],
                        doc_type=rec["es_type"],
                        id=rec["body"]["_id"],
                        parent=rec["body"]["parent"],
                        body=rec["body"]
                    )
                else:
                    es.index(
                        index=rec["es_index"],
                        doc_type=rec["es_type"],
                        id=rec["body"]["_id"],
                        body=rec["body"]
                    )
