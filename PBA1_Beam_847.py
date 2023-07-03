from __future__ import absolute_import
import argparse
import logging
import apache_beam as beam
import numpy as np
from apache_beam.io import ReadFromText
from apache_beam.transforms import window
from apache_beam.transforms import window
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class KeyValuePair(beam.DoFn):
    # Use classes and functions to perform transformations on your PCollections
    # Yield the element(s) needed as input for the next transform
    def process(self, element: object) -> object:
        list_element = element.split(",")
        if len(list_element) > 2:
            # e_dict = {list_element[0]: list_element[1:]}
            test = (list_element[1], list_element[2:])
        else:
            # e_dict = {list_element[-1]: list_element[0]}
            test = (list_element[-1], list_element[1])
        element = test
        yield element


class Gender(beam.DoFn):
    def process(self, element: object, *args: object, **kwargs: object) -> object:
        user_info = element[1]
        total_dict = user_info["users"][0][0], len(user_info["orders"])
        return [total_dict]


class Bytes(beam.DoFn):
    def process(self, element: object, *args: object, **kwargs: object) -> object:
        timestamp_info = element[1]
        bytes_info = []
        for reading in timestamp_info:
            if reading[0] == 'bytes':
                pass
            else:
                bytes_info.append(float(reading[0]))
        test = (element[0], float(np.array(bytes_info).mean()))
        # test = (element[0],bytes_info)
        return [test]


def run(argv: object = None, save_main_session: object = True) -> object:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input-first-file',
        dest='input_1',
        required=True,
        help='Input file to process.')
    parser.add_argument(
        '--input-second-file',
        dest='input_2',
        required=True,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # Create the initial PCollection
    p = beam.Pipeline(options=pipeline_options)

    # Read the input file into a PCollection & perform transformations
    users_dict = (p
                  | 'first read' >> ReadFromText(known_args.input_1)
                  | 'dictionary' >> beam.ParDo(KeyValuePair())
                  )

    # question 1
    question_1 = (users_dict
                  # | "group by customer" >> beam.GroupByKey
                  | 'window data' >> beam.WindowInto(window.SlidingWindows(10, 30))
                  # | "get bytes per timestamp" >> beam.ParDo(Bytes())
                  # | "combine by key" >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
                  | "print" >> beam.Map(print))
    #
    # question_1 = (users_dict | "group by customer" >> beam.GroupByKey()
    #               | "print" >> beam.Map(print))

    # question_2
    # question_2 = ({"users": users_dict, "orders": orders_dict}
    #               | "group by customer" >> beam.CoGroupByKey()
    #               | "get gender for each customer" >> beam.ParDo(Gender())
    #               | "combine by key" >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
    #               | "print" >> beam.Map(print))

    # question_3
    # question_3 = ({"users": users_dict, "orders": orders_dict}
    #               | "group by customer" >> beam.CoGroupByKey()
    #               | "add age grouping" >> beam.ParDo(Age_grouping())
    #               | "count total order per grouping" >> beam.CombinePerKey(sum)
    #               | "print" >> beam.Map(print))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
