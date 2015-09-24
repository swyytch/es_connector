#!/usr/bin/env python2

from __future__ import print_function
import logging
import sys
import yaml

from elasticsearch import Elasticsearch
from pymongo import MongoClient
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
        "swyytch",
        "3L9t8KeqQ3I96^M159^@93O3PCZV*4X7aSEmlZrTHzkGHe^F",
        mechanism='SCRAM-SHA-1'
    )
    return db


def prepare(rec, dbname, collection):
    rec["timestamp"] = time()
    rec["_id"] = str(rec["_id"])
    if "parent" in rec:
        rec["parent"] = str(rec["parent"])

    return {"body": rec, "db": dbname, "col": collection}


def get_es_values(rec):
    # if rec["db"] == "bernie":
    #     name = [rec["col"], rec["body"]["lang"], VERSION]
    #     rec["es_index"] = "_".join(name)
    #     rec["es_type"] = rec["body"]["site"].replace(".", "_")
    # elif rec["db"] == "facebook":
    #     if rec["col"] == "token":
    #         return False
    #     name = ["fb", rec["body"]["page"], VERSION]
    #     rec["es_index"] = "_".join(name)
    #     rec["es_type"] = rec["col"] if rec["col"] != "data" else "stats"
    if rec["db"] == "campaign":
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
    for dbname in ["bernie", "facebook"]:
        for collection in db[dbname].collection_names():
            if collection in ["system.indexes"]:
                continue
            cursor = db[dbname][collection].find()
            for doc in cursor:
                rec = prepare(doc, dbname, collection)
                if not rec:
                    continue
                rec = get_es_values(rec)
                if not rec:
                    continue
                if rec["col"] == "events":
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
