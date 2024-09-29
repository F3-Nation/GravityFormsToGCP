import os
import requests
import logging

class GoogleSheets:
    WORKOUT_HISTORY_SPREADSHEETURL = os.getenv('WORKOUT_HISTORY_SPREADSHEETURL')

    def get_all_workouts(self) -> list:
        param = {"dataset":"workouts"}
        response = requests.get(self.WORKOUT_HISTORY_SPREADSHEETURL, params=param)
        response.encoding = 'utf-8-sig'
        body = response.json()

        if body["Status"] != 200:
            logging.error("Failed to successfully pull historical workout data from Google Sheets. Error: " + body["Message"])
            return {}

        data = body["Data"]
        output = []
        for row in data[1:]:
            if (len(row[0]) == 0):
                break
            
            formattedRow = {
                'workoutname': row[0],
                'region': row[1],
                'starttime': row[2],
                'workouttype': row[3],
                'latitude': row[4],
                'longitude': row[5],
                'weekday': row[6],
                'note': row[7],
                'websiteurl': row[8],
                'logourl': row[9],
                'address1': row[10],
                'address2': row[11],
                'city': row[12],
                'state': row[13],
                'postalcode': row[14],
                'country': row[15],
                'addressaccurate': self.convert_to_boolean(row[16]),
                'stationary': self.convert_to_boolean(row[17]),
                'submittername': row[18],
                'submitteremail': row[19],
                'entryid': row[20],
                'createdtimestamp': self.remove_non_timestamp(row[21]),
                'updatedtimestamp': self.remove_non_timestamp(row[22]),
                'isapproved': row[23]
            }
            output.append(formattedRow)
        return output
    
    def convert_to_boolean(self, input: str) -> bool|None:
        if (input == "Yes"):
            return True
        elif (input == "No"):
            return False
        else:
            return None

    def remove_non_timestamp(self, input: str) -> str|None:
        if (input == "0000-00-00 00:00:00"):
            return None
        else:
            return input