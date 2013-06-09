#!/usr/bin/env dumbo

"""
David F. Gleich
Copyright (c) 2013
"""

import mrmc
import dumbo


def runner(job):
    mapper = mrmc.ID_MAPPER
    reducer = mrmc.ID_REDUCER
    job.additer(mapper=mapper, reducer=reducer,
                opts=[('numreducetasks', str(1))])

if __name__ == '__main__':
    dumbo.main(runner)
