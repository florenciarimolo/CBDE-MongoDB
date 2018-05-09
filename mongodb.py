from pymongo import MongoClient
from pymongo.database import Collection, CommandCursor

DATABASE_NAME = 'CBDE'


def create_lineitem(data: dict):
    return {
        '_id': "{}_{}".format(data['l_orderkey'], data['l_linenumber']),
        'l_orderkey': data['l_orderkey'],
        'l_returnflag': data['l_returnflag'],
        'l_linestatus': data['l_linestatus'],
        'l_quantity': data['l_quantity'],
        'l_extendedprice': data['l_extendedprice'],
        'l_discount': data['l_discount'],
        'l_tax': data['l_tax'],
        'l_shipdate': data['l_shipdate'],
        'o_orderdate': data['o_orderdate'],
        'o_shippriority': data['o_shippriority'],
        'c_mktsegment': data['c_mktsegment'],
        'c_nationkey': data['c_nationkey'],
        's_nationkey': data['s_nationkey'],
        'r_name': data['r_name'],
        'n_name': data['n_name']
    }


def create_partsup(data: dict):
    return {
        '_id': "{}_{}".format(data['ps_partkey'], data['ps_suppkey']),
        'p_partkey': data['ps_partkey'],
        'p_mfgr': data['p_mfgr'],
        'p_size': data['p_size'],
        'p_type': data['p_type'],
        's_acctbal': data['s_acctbal'],
        's_name': data['s_name'],
        's_address': data['s_address'],
        's_phone': data['s_phone'],
        's_comment': data['s_comment'],
        'n_name': data['n_name'],
        'r_name': data['r_name'],
        'ps_supplycost': data['ps_supplycost']
    }


def query1(collection: Collection, date: str):
    return collection.aggregate([
        {"$match": {
            "l_shipdate": {"$lte": date}
        }},
        {"$project": {
            "l_returnflag": "$l_returnflag",
            "l_linestatus": "$l_linestatus",
            "l_quantity": "$l_quantity",
            "l_extendedprice": "$l_extendedprice",
            "l_discount": "$l_discount",
            "l_tax": "$l_tax"
        }},
        {"$group": {
            "_id": {"l_returnflag": "$l_returnflag", "l_linestatus": "$l_linestatus"},
            "l_returnflag": {"$first": "$l_returnflag"},
            "l_linestatus": {"$first": "$l_linestatus"},
            "sum_qty": {"$sum": "$l_quantity"},
            "sum_base_price": {"$sum": "$l_extendedprice"},
            "sum_disc_price": {"$sum": {"$multiply": ["$l_extendedprice", {"$subtract": [1, "$l_discount"]}]}},
            "sum_charge": {"$sum": {
                "$multiply": [{"$multiply": ["$l_extendedprice", {"$subtract": [1, "$l_discount"]}]},
                              {"$add": [1, "$l_tax"]}]}},
            "avg_qty": {"$avg": "$l_quantity"},
            "avg_price": {"$avg": "$l_extendedprice"},
            "avg_disc": {"$avg": "$l_discount"},
            "count_order": {"$sum": 1}
        }},
        {"$sort": {
            "l_returnflag": 1,
            "l_linestatus": 1
        }}
    ])


def query2(collection: Collection, size: int, type: str, region: str):
    return collection.aggregate([
        {"$match": {
            "$and": [
                {"p_size": {"$eq": size}},
                {"p_type": {"$regex": type}},
                {"r_name": {"$eq": region}},
                {"ps_supplycost": {"$eq": query2_subquery(collection, region, size, type)}}
            ]
        }},
        {"$project": {
            "s_acctbal": "$s_acctbal",
            "s_name": "$s_name",
            "n_name": "$n_name",
            "p_partkey": "$p_partkey",
            "p_mfgr": "$p_mfgr",
            "s_address": "$s_address",
            "s_phone": "$s_phone",
            "s_comment": "$s_comment"
        }},
        {"$sort": {
            "s_acctbal": -1,
            "n_name": 1,
            "s_name": 1,
            "p_partkey": 1
        }}
    ])


def query2_subquery(collection: Collection, region: str, size: int, type: str):
    return collection.aggregate([
        {"$match": {
            "$and": [
            {"r_name": {"$eq": region}},
            {"p_size": {"$eq": size}},
            {"p_type": {"$regex": type}}
            ]
        }},
        {"$project": {
            "ps_supplycost": "$ps_supplycost"
        }},
        {"$group": {
            "_id": None,
            "min_supplycost": {"$min": "$ps_supplycost"}
        }}
    ]).next()['min_supplycost']


def query3(collection: Collection, segment: str, date1: str, date2: str):
    return collection.aggregate([
        {"$match": {
            "$and": [
                {"c_mktsegment": {"$eq": segment}},
                {"o_orderdate": {"$lt": date1}},
                {"l_shipdate": {"$gt": date2}}
            ]
        }},
        {"$project": {
            "l_orderkey": "$l_orderkey",
            "l_extendedprice": "$l_extendedprice",
            "l_discount": "$l_discount",
            "o_orderdate": "$o_orderdate",
            "o_shippriority": "$o_shippriority"
        }},
        {"$group": {
            "_id": {"l_orderkey": "$l_orderkey", "o_orderdate": "$o_orderdate", "o_shippriority": "$o_shippriority"},
            "l_orderkey": {"$first": "$l_orderkey"},
            "revenue": {"$sum": {"$multiply": ["$l_extendedprice", {"$subtract": [1, "$l_discount"]}]}},
            "o_orderdate": {"$first": "$o_orderdate"},
            "o_shippriority": {"$first": "$o_shippriority"}
        }},
        {"$sort": {
            "revenue": -1,
            "o_orderdate": 1
        }}
    ])


def query4(collection: Collection, date: str, region: str):
    return collection.aggregate([
        {"$match": {
            "$and": [
                {"r_name": {"$eq": region}},
                {"o_orderdate": {"$gte": date}},
                {"o_orderdate": {"$lt": date[:3] + str(int(date[3])+1) + date[4:]}}
            ]
        }},
        {"$redact":
            {"$cond": [
                {"$eq": ["$s_nationkey", "$c_nationkey"]},
                "$$KEEP", "$$PRUNE"
            ]
            }},
        {"$project": {
            "n_name": "$n_name",
            "l_extendedprice": "$l_extendedprice",
            "l_discount": "$l_discount"
        }},
        {"$group": {
            "_id": {"n_name": "$n_name"},
            "n_name": {"$first": "$n_name"},
            "revenue": {"$sum": {"$multiply": ["$l_extendedprice", {"$subtract": [1, "$l_discount"]}]}}
        }},
        {"$sort": {
            "revenue": -1
        }}
    ])



def create_lineitem_collection():
    print('Creating LineItem collection...')

    client = MongoClient()
    client.drop_database(DATABASE_NAME)

    db = client.get_database(DATABASE_NAME)
    return db.get_collection('LINEITEM')


def create_partsupp_collection():
    print('Creating PartSupp collection...')

    client = MongoClient()
    client.drop_database(DATABASE_NAME)

    db = client.get_database(DATABASE_NAME)
    return db.get_collection('PARTSUPP')


def insert_data(collection_lineitem: Collection, collection_partsupp: Collection):
    print("Inserting documents...")

    data1 = {
        'l_orderkey': 1,
        'l_linenumber': 1,
        'l_returnflag': "R",
        'l_linestatus': "R",
        'l_quantity': 2.5,
        'l_extendedprice': 140.0,
        'l_discount': 0.1,
        'l_tax': 0.1,
        'l_shipdate': "2018-01-24",
        'o_orderdate': "2018-01-29",
        'o_shippriority': 1,
        'c_mktsegment': "SEGMENT1",
        'c_nationkey': 1,
        's_nationkey': 1,
        'r_name': "REGION1",
        'n_name': "NATION1"
    }

    data2 = {
        'l_orderkey': 2,
        'l_linenumber': 2,
        'l_returnflag': "R",
        'l_linestatus': "R",
        'l_quantity': 2.5,
        'l_extendedprice': 140.0,
        'l_discount': 0.1,
        'l_tax': 0.1,
        'l_shipdate': "2018-04-24",
        'o_orderdate': "2018-04-29",
        'o_shippriority': 1,
        'c_mktsegment': "SEGMENT2",
        'c_nationkey': 1,
        's_nationkey': 1,
        'r_name': "REGION1",
        'n_name': "NATION1"
    }

    data3 = {
        'l_orderkey': 3,
        'l_linenumber': 3,
        'l_returnflag': "R",
        'l_linestatus': "R",
        'l_quantity': 2.5,
        'l_extendedprice': 140.0,
        'l_discount': 0.1,
        'l_tax': 0.1,
        'l_shipdate': "2018-03-24",
        'o_orderdate': "2018-03-29",
        'o_shippriority': 1,
        'c_mktsegment': "SEGMENT1",
        'c_nationkey': 2,
        's_nationkey': 2,
        'r_name': "REGION2",
        'n_name': "NATION2"
    }

    data4 = {
        'l_orderkey': 4,
        'l_linenumber': 4,
        'l_returnflag': "R",
        'l_linestatus': "R",
        'l_quantity': 2.5,
        'l_extendedprice': 140.0,
        'l_discount': 0.1,
        'l_tax': 0.1,
        'l_shipdate': "2018-03-14",
        'o_orderdate': "2018-03-19",
        'o_shippriority': 1,
        'c_mktsegment': "SEGMENT2",
        'c_nationkey': 2,
        's_nationkey': 9,
        'r_name': "REGION2",
        'n_name': "NATION2"
    }

    data = [data1, data2, data3, data4]

    for datum in data:
        document = create_lineitem(datum)
        collection_lineitem.insert_one(document)

    data5 = {
        'ps_partkey': 1,
        'ps_suppkey': 1,
        'p_mfgr': 'MFGR',
        'p_size': 23,
        'p_type': 'TYPE2',
        's_acctbal': 2.34,
        's_name': 'SUPPLIER',
        's_address': 'AN ADDRESS',
        's_phone': '000000000',
        's_comment': 'COMMENT',
        'n_name': 'NATION1',
        'r_name': 'REGION1',
        'ps_supplycost': 34.34
    }

    data6 = {
        'ps_partkey': 2,
        'ps_suppkey': 2,
        'p_mfgr': 'MFGR',
        'p_size': 90,
        'p_type': 'TYPE2',
        's_acctbal': 2.34,
        's_name': 'SUPPLIER',
        's_address': 'AN ADDRESS',
        's_phone': '000000000',
        's_comment': 'COMMENT',
        'n_name': 'NATION1',
        'r_name': 'REGION1',
        'ps_supplycost': 34.34
    }

    data7 = {
        'ps_partkey': 3,
        'ps_suppkey': 3,
        'p_mfgr': 'MFGR',
        'p_size': 23,
        'p_type': 'TYPE1',
        's_acctbal': 2.34,
        's_name': 'SUPPLIER',
        's_address': 'AN ADDRESS',
        's_phone': '000000000',
        's_comment': 'COMMENT',
        'n_name': 'NATION1',
        'r_name': 'REGION1',
        'ps_supplycost': 34.34
    }

    data8 = {
        'ps_partkey': 4,
        'ps_suppkey': 4,
        'p_mfgr': 'MFGR',
        'p_size': 90,
        'p_type': 'TYPE2',
        's_acctbal': 2.34,
        's_name': 'SUPPLIER',
        's_address': 'AN ADDRESS',
        's_phone': '000000000',
        's_comment': 'COMMENT',
        'n_name': 'NATION1',
        'r_name': 'REGION1',
        'ps_supplycost': 34.34
    }

    data = [data5, data6, data7, data8]

    for datum in data:
        document = create_partsup(datum)
        collection_partsupp.insert_one(document)

    print('Documents inserted successfully.')


def print_result(query: int, result: CommandCursor):
    print('\n')
    print("---------------------------------- QUERY " + str(query) + " -------------------------------------")
    print('\n')

    for row in result:
        print(row)

    print('\n')


def execute_queries(collection_lineitem: Collection, collection_partsupp: Collection):
    print('Executing queries...')
    result1 = query1(collection_lineitem, "2018-04-24")
    print_result(1, result1)

    result2 = query2(collection_partsupp, 23, "TYPE2", "REGION1")
    print_result(2, result2)

    result3 = query3(collection_lineitem, "SEGMENT1", "2018-04-30", "2018-01-23")
    print_result(3, result3)

    result4 = query4(collection_lineitem, "2018-01-29", "REGION1")
    print_result(4, result4)


def main():
    collection_lineitem = create_lineitem_collection()
    print('Collection inserted successfully.')
    collection_partsupp = create_partsupp_collection()
    print('Collection inserted successfully.')
    insert_data(collection_lineitem, collection_partsupp)
    execute_queries(collection_lineitem, collection_partsupp)


if __name__ == "__main__":
    main()
