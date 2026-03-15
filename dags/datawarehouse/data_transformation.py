from datetime import timedelta, datetime
import logging

logger = logging.getLogger(__name__)

def parse_duration(duration_str):
    try:
        
        # Replace method will still work even if the "T" character is in the string
        duration_str = duration_str.replace("P", "").replace("T", "")
        
        # Days, Hours, Minutes, Seconds
        components = ["D", "H", "M", "S"]
        values = {"D" : 0, "H" : 0, "M" : 0, "S" : 0}

        for comp in components:
            if comp in duration_str:
                value, duration_str = duration_str.split(comp)
                values[comp] = int(value)
        
        total_duration = timedelta(
            days = values["D"],
            hours = values["H"],
            minutes = values["M"],
            seconds = values["S"]
        )

        logger.info(f"Succesfully parsed duration")

        return total_duration

    except Exception as e:
        logger.error(f"Failed parsing of duration - {e}")
        raise e


def transform_data(row):

    duration_td = parse_duration(row["Duration"])

    # Earliest possible datetime: 00:00:00
    row["Duration"] = (datetime.min + duration_td).time()

    # Video Type will be categorized as follows: Shorts if the duration is equal or less than 1 min, if not it is Normal
    row["Video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"

    return row
