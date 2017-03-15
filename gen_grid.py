#! /bin/python3
# -*- codding=utf-8 -*-



def floatrange(start,end,step):
    temp = start
    while temp < end:
        yield temp
        temp += step


def gen_grid(outpath = None,min_lon=73,min_lat=4,max_lon=135,max_lat=53,inter_lon=0.1,inter_lat=0.1):
    if not outpath:
        return


if __name__ == '__main__':
    gen_grid(outpath='../data/grid.csv')