import apache_beam as beam
from apache_beam.transforms.window import (
    TimestampedValue,
    Sessions,
    Duration,
)
from apache_beam.io.textio import WriteToText

# User defined functions should always be subclassed from DoFn. This function transforms
# each element into a tuple where the first field is userId and the second is click. It
# assigns the timestamp to the metadata of the element such that window functions can use
# it later to group elements into windows.
class AddTimestampDoFn(beam.DoFn):
    def process(self, element):
        unix_timestamp = element["timestamp"]
        element = (element["userId"], element["click"])

        yield TimestampedValue(element, unix_timestamp)


with beam.Pipeline() as p:
    # fmt: off
    events = p | beam.Create(
        [
            {"userId": "Andy", "click": 1, "timestamp": 1603112520},  # Event time: 13:02
            {"userId": "Sam", "click": 1, "timestamp": 1603113240},  # Event time: 13:14
            {"userId": "Andy", "click": 1, "timestamp": 1603115820},  # Event time: 13:57
            {"userId": "Andy", "click": 1, "timestamp": 1603113600},  # Event time: 13:20
        ]
    )
    # fmt: on

    # Assign timestamp to metadata of elements such that Beam's window functions can
    # access and use them to group events.
    timestamped_events = events | "AddTimestamp" >> beam.ParDo(AddTimestampDoFn())

    windowed_events = timestamped_events | beam.WindowInto(
        # Each session must be separated by a time gap of at least 30 minutes (1800 sec)
        Sessions(gap_size=30 * 60),
        # Triggers determine when to emit the aggregated results of each window. Default
        # trigger outputs the aggregated result when it estimates all data has arrived,
        # and discards all subsequent data for that window.
        trigger=None,
        # Since a trigger can fire multiple times, the accumulation mode determines
        # whether the system accumulates the window panes as the trigger fires, or
        # discards them.
        accumulation_mode=None,
        # Policies for combining timestamps that occur within a window. Only relevant if
        # a grouping operation is applied to windows.
        timestamp_combiner=None,
        # By setting allowed_lateness we can handle late data. If allowed lateness is
        # set, the default trigger will emit new results immediately whenever late
        # data arrives.
        allowed_lateness=Duration(seconds=1 * 24 * 60 * 60),  # 1 day
    )

    # We can use CombinePerKey with the predifined sum function to combine all elements 
    # for each key in a collection.
    sum_clicks = windowed_events | beam.CombinePerKey(sum)

    # WriteToText writes a simple text file with the results.
    sum_clicks | WriteToText(file_path_prefix="output")
