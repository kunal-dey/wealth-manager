import json
from datetime import datetime

HOLIDAYS: dict[str, list[datetime]] = {
    "dates": []
}


def load_holidays():
    global HOLIDAYS
    with open("temp/holidays.json") as file:
        try:
            HOLIDAYS['dates'] = []
            for date_str in json.load(file)['dates']:
                HOLIDAYS['dates'].append(datetime.strptime(date_str, "%Y-%m-%d"))
        except FileNotFoundError:
            print("error while loading the file")
    return HOLIDAYS

