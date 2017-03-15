#! /bin/python
# -*- coding:utf-8 -*-



from pyspark import SparkContext,SparkConf
from math import floor


class GridIndex(object):
    """
    全球剖分,详见程承旗老师的全球剖分理论~哈哈哈
    其实就是以0,0为起点,给定经度步长和纬度步长然后把地球划分为固定的小格子就行了

    """

    def __init__(self,_lon_inter = 1, _lat_inter = 1):
        self.lon_inter = _lon_inter
        self.lat_inter = _lat_inter

    def get_index_point(self,lon,lat):
        return '%d_%d' % (lon/self.lon_inter,lat/self.lat_inter)

    def get_index_bbox(self,minlon,minlat,maxlon,maxlat):
        minlon_index = minlon / self.lon_inter
        minlat_index = minlat / self.lat_inter
        maxlon_index = maxlon / self.lon_inter
        maxlat_index = minlat / self.lat_inter
        result = []
        for i in (minlon_index,maxlon_index+1,1):
            for j in (minlat_index,maxlat_index+1,1):
                result.append('%d_%d' % (i,j))
        return result



sc = None

def initspark(appname='test',master='local'):
    conf = SparkConf().setAppName(appname).setMaster(master)
    global sc
    if sc is None:
        sc = SparkContext(conf=conf)
    return sc

# spark 2.0
from pyspark.sql import SparkSession
from pyspark.sql.types import *
def csv2parquet(filepath,spatial_index = False):
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    def mapfun(x):
        items = x.strip().split(',')
        return (int(items[0]),x)
    gridindex = GridIndex()
    def map_spatialindex_fun(x):
        num,lon,lat = [float(i) for i in x.strip().split(',')]
        return (gridindex.get_index_point(lon,lat),x)
    if spatial_index:
        rdd = spark.sparkContext.textFile(filepath).map(map_spatialindex_fun)
    else:
        rdd = spark.sparkContext.textFile(filepath).map(mapfun)

    schemaString = 'myindex num_lon_lat'
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)
    df = spark.createDataFrame(rdd,schema)
    # df.filter(df['index'] < 10).show()
    df.write.parquet('../data/test_point.parquet/')



def csv_point_fun(rdd_point):
    gridindex = GridIndex()
    def map_point_handle(x):
        items = [float(a) for a in x.strip().split(',')]
        lon = items[1]
        lat = items[2]
        return (gridindex.get_index_point(lon, lat), x)
    kv_point_rdd = rdd_point.map(map_point_handle)
    return kv_point_rdd

def parquet_point_fun(rdd_point):
    def map_point_handle(x):
        spatial_index = x.myindex
        val = x.num_lon_lat
        return (spatial_index,val)
    kv_point_rdd = rdd_point.map(map_point_handle)
    return kv_point_rdd

def csv_grid_fun(rdd_grid):
    gridindex = GridIndex()
    def map_bbox_handle(x):
        minlon, minlat, maxlon, maxlat = [float(a) for a in x.strip().split(',')]
        l = gridindex.get_index_bbox(minlon, minlat, maxlon, maxlat)
        final_list = []
        for i in l:
            final_list.append((i, '%f,%f,%f,%f' % (minlon, minlat, maxlon, maxlat)))
        return final_list

    kv_grid_rdd = rdd_grid.flatMap(map_bbox_handle)
    return kv_grid_rdd

def parquet_grid_fun(rdd_grid):
    gridindex = GridIndex()
    def map_bbox_handle(x):
        minlon, minlat, maxlon, maxlat = [float(a) for a in x.value.strip().split(',')]
        l = gridindex.get_index_bbox(minlon, minlat, maxlon, maxlat)
        final_list = []
        for i in l:
            final_list.append((i, '%f,%f,%f,%f' % (minlon, minlat, maxlon, maxlat)))
        return final_list

    kv_grid_rdd = rdd_grid.flatMap(map_bbox_handle)
    return kv_grid_rdd


def spatial_join(point_fun,grid_fun,rdd_point,rdd_grid):
    """
    传入两个闭包,分别用于得到给定格式的rdd,具体解析方式由外部定义
    :param point_fun:
    :param grid_fun:
    :return:
    """
    from datetime import datetime
    starttime = datetime.now()
    print('start to spatial_join at time %s ' % starttime)

    kv_point_rdd = point_fun(rdd_point)
    kv_grid_rdd = grid_fun(rdd_grid)

    def map_decare(x):
        key = x[0]
        points = x[1][0]
        grids = x[1][1]
        result = []
        for point in points:
            num, lon, lat = [float(i) for i in point.strip().split(',')]

            for grid in grids:
                minlon, minlat, maxlon, maxlat = [float(i) for i in grid.strip().split(',')]
                if lon >= minlon and lon < maxlon and lat >= minlat and lat < maxlat:
                    result.append((point, grid))
                    # print(result[len(result)-1])
        return result

    joinedrdd = kv_point_rdd.cogroup(kv_grid_rdd).flatMap(map_decare)
    joinedrdd.cache()
    # start to calculate
    # joinedrdd.collect()
    joinedrdd.count()
    finishtime = datetime.now()
    print('finish spatial join at %s ' % finishtime)
    consumetime = finishtime - starttime
    print('consum_time = %d secs' % consumetime.seconds)
    res = joinedrdd.take(100)
    for r in res:
        print(r)
    finishtime2 = datetime.now()
    consumetime2 = finishtime2 - finishtime
    print('consum_time_2 = %d secs' % consumetime2.seconds)


# def spatial_join(pointrdd,gridrdd):
#     """
#     空间join,先进行格网索引,然后用cogroup进行分组,即把每个网格内的空间对象放到同一个地方,然后对格网内的对象做相交运算。
#     :param pointrdd:
#     :param gridrdd:
#     :return:
#     """
#     starttime = datetime.now()
#     print('start to spatial_join at time %s ' % starttime)
#     gridindex = GridIndex()
#     def map_point_handle(x):
#         items = [float(a) for a in x.strip().split(',')]
#         lon = items[1]
#         lat = items[2]
#         return (gridindex.get_index_point(lon,lat),x)
#     kv_point_rdd = pointrdd.map(map_point_handle)
#
#     def map_bbox_handle(x):
#         minlon,minlat,maxlon,maxlat = [float(a) for a in x.strip().split(',')]
#         l = gridindex.get_index_bbox(minlon,minlat,maxlon,maxlat)
#         final_list = []
#         for i in l:
#             final_list.append((i,'%f,%f,%f,%f' % (minlon,minlat,maxlon,maxlat)))
#         return final_list
#     kv_grid_rdd = gridrdd.flatMap(map_bbox_handle)
#
#     def map_decare(x):
#         key = x[0]
#         points = x[1][0]
#         grids = x[1][1]
#         result = []
#         for point in points:
#             num,lon,lat = [float(i) for i in point.strip().split(',')]
#
#             for grid in grids:
#                 minlon,minlat,maxlon,maxlat = [float(i) for i in grid.strip().split(',')]
#                 if lon >= minlon and lon < maxlon and lat >= minlat and lat < maxlat:
#                     result.append((point,grid))
#                     # print(result[len(result)-1])
#         return result
#
#     joinedrdd = kv_point_rdd.cogroup(kv_grid_rdd).flatMap(map_decare)
#     joinedrdd.cache()
#     #start to calculate
#     # joinedrdd.collect()
#     joinedrdd.count()
#     finishtime = datetime.now()
#     print('finish spatial join at %s ' % finishtime)
#     consumetime = finishtime - starttime
#     print('consum_time = %d secs' % consumetime.seconds)
#     res = joinedrdd.take(100)
#     for r in res:
#         print(r)
#     finishtime2 = datetime.now()
#     consumetime2 = finishtime2 - finishtime
#     print('consum_time_2 = %d secs' % consumetime2.seconds)


# def wordcount(filepath):
#     spark = initspark(appname='wordcount')
#     rdd = spark.textFile(filepath)
#     count = rdd.flatMap(lambda x: x.split()).map(lambda x: [x,1]).reduceByKey(lambda x,y:x+y)
#     output = count.take(10)
#     for word in output:
#         print word


def testcsv(path_csv,path_grid):
    """
    be careful: rdd is
    :param path_csv:
    :param path_grid:
    :return:
    """
    spark = initspark()
    pointrdd = spark.textFile('../data/test_point.csv')
    gridrdd = spark.textFile('../data/grid.csv')
    spatial_join(csv_point_fun,csv_grid_fun,pointrdd,gridrdd)

def testparquet(path_parquet,path_grid):
    """
    be careful: spark 2.0+ needed
    input filepath and return rdd
    rdd contians row object
    :return:
    """
    from pyspark.sql import SparkSession
    spark = SparkSession \
        .builder \
        .appName("readparquet") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    parquetFile = spark.read.parquet(path_parquet)
    rdd_point = parquetFile.rdd
    rdd_grid = spark.read.text(path_grid).rdd

    spatial_join(parquet_point_fun,parquet_grid_fun,rdd_point,rdd_grid)


if __name__=='__main__':
    # #test local csv
    # spark = initspark(appname='test')
    # pointrdd = spark.textFile('../data/test_point.csv')
    # gridrdd = spark.textFile('../data/grid.csv')
    # spatial_join(pointrdd,gridrdd)


    # #csv2parquet
    # csv2parquet('../data/test_point.csv',spatial_index=True)

    # #test local parquet
    # pointrdd = readparquet('../data/test_point.parquet')
    # res = pointrdd.take(10)
    # for r in res:
    #     print(r)


    # # test spatial-join of parquet file
    # testparquet('../data/test_point.parquet','../data/grid.csv')

    # test spatial-join of csv file
    testcsv('../data/test_point.csv','../data/grid.csv')

