import csv
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

# KOVAAKs LEADERBOARD IDs
Leaderboard_ID = [
 605,405,833,68,319,108,370, 42,510,
 603,520, 568, 538, 960, 577, 542, 543,868,
 6,9,8,68,319,108,300,30,99,
 363,316,29,464,1114,26,604,947,596
]

# VOLT REQUIREMENTS
VoltsReq = [
 [68, 116.8],
 [78, 128],
 [220, 490],
 [130, 175],
 [115, 158],
 [152, 220],
 [2600, 4212],
 [2700, 4245],
 [1100, 2755],
 [11300, 17050],
 [862, 897.9],
 [850, 891.6],
 [91, 120.8],
 [85, 118.1],
 [102, 135],
 [65, 95.5],
 [64, 97],
 [50, 70.9],
]

# EASY VOLT REQUIREMENTS
VoltsReqE = [
 [40, 75],
 [52, 86],
 [100, 310],
 [83, 123],
 [70, 110],
 [95, 142],
 [1700, 3100],
 [1800, 3300],
 [700, 1900],
 [7500, 14000],
 [835, 880],
 [775, 865],
 [65, 98],
 [50, 85],
 [74, 109],
 [45, 72],
 [40, 77],
 [35, 54],
]

# S3 RANK REQUIREMENTS
RankReq = [
 [0,40, 50, 56, 66, 75, 68, 76, 85, 95, 105, 110.2],
 [0,52, 61, 70, 77, 86, 78, 88, 98, 108, 115, 123],
 [0,100, 150, 200, 250, 310, 220, 260, 320, 390, 440, 450],
 [0,83, 93, 103, 113, 123, 130, 138, 148, 160, 170, 172],
 [0,70, 82, 91, 100, 110, 115, 120, 130, 142, 152, 156],
 [0,95, 108, 120, 132, 142, 152, 160, 175, 192, 210, 213],
 [0,1700,	2100, 2400,	2800, 3100, 2600, 2900, 3400, 3800, 4000, 4134],
 [0,1800,	2200, 2600,	3000, 3300, 2700, 3000, 3400, 3900, 4100, 4226],
 [0,700, 1000, 1300,	1600, 1900, 1100, 1400, 1800, 2200, 2500, 2588],
 [0,7500,	8500, 10000, 12000,	14000, 11300, 12500, 13800, 15200, 16000, 16840],
 [0,835, 845,	855, 870, 880, 862, 870, 880, 888, 894, 896],
 [0,775, 800,	825, 852, 865, 850, 860, 872, 883, 887, 890.7],
 [0,65, 80, 85, 91, 98, 91, 97, 103, 110, 116, 118.2],
 [0,50, 58, 67, 76, 85, 85, 90, 98, 107, 114, 117.5],
 [0,74, 82, 90, 100, 109, 102, 110, 118, 125, 131, 133],
 [0,45, 54, 60, 66, 72, 65, 72, 79, 84, 88, 91],
 [0,40, 51, 60, 69, 77, 64, 70, 78, 85, 92, 95.8],
 [0,35, 42, 46, 50, 54, 50, 55, 60, 65, 70, 70.4],
]

# S3 RANKS
Ranks = ["N/A", "Unranked", "Bronze", "Silver", "Gold", "Platinum", "Diamond", "Jade", "Master", "Grandmaster", "Nova", "Astra", "Celestial"]

# FUNCTION TO PROCESS EACH PAGE OF EACH LEADERBOARD (FUNCTION CALLED VIA THREADING)
def process_leaderboard(leaderboard_id, page, session, itera, Count, score_lock, Score_Dic, RankReq, VoltsReq):
    result = []

    # API DATA PULL
    try:
        r = session.get(f"https://kovaaks.com/webapp-backend/leaderboard/scores/global?leaderboardId={leaderboard_id}&page={page}&max=100").json()
        print(f"Leaderboard {leaderboard_id}. Page: {page} data pull.")

        # ITERATE THROUGH ALL DATA ROWS (100 LEADERBOARD ENTRIES) IN THE API PULL
        for Data in r['data']:
            try:
                Steam_Name = Data['steamAccountName']
                Steam_ID = Data['steamId']
                Score = Data['score']

                # LOCK
                with score_lock:

                    # IF STEAM ID WAS NOT YET SEEN CREATE KEY AND SET VOLTS TO ZERO
                    if Steam_ID not in Score_Dic:
                        Score_Dic[Steam_ID] = [-2] * (len(Leaderboard_ID) + 6)
                        Score_Dic[Steam_ID][38] = Steam_Name
                        Score_Dic[Steam_ID][18] = 0  # Volts
                        Score_Dic[Steam_ID][39] = 0  # Easy Volts

                    # FOR EASY LEADERBOARDS
                    if itera == 1:

                        # ITERATE THROUGH RANKS
                        for iii in range(0, 6):
                            if RankReq[Count][iii] <= Score:
                                Score_Dic[Steam_ID][19 + Count] = iii - 1
                                Score_Dic[Steam_ID][Count] = Score
                        VoltsE = min(max(Score - VoltsReqE[Count][0], 0) / max(VoltsReqE[Count][1] - VoltsReqE[Count][0], 1) * 100, 100)
                        Score_Dic[Steam_ID][39] += VoltsE

                    # FOR NORMAL LEADERBOARD
                    elif itera == 2:

                        # ITERATE THROUGH RANKS
                        for iii in range(6, 12):
                            if RankReq[Count][iii] <= Score:
                                Score_Dic[Steam_ID][19 + Count] = iii-1
                                Score_Dic[Steam_ID][Count] = Score
                        Volts = min(max(Score - VoltsReq[Count][0], 0) / max(VoltsReq[Count][1] - VoltsReq[Count][0], 1) * 100, 100)
                        Score_Dic[Steam_ID][18] += Volts

            except KeyError:
                continue
    except Exception as e:
        print(f"Error processing leaderboard {leaderboard_id} page {page}: {e}")
    return result

# Main code with threading and lock protection
Score_Dic = {}
score_lock = Lock()  # Create a lock for protecting shared resources

# START THREADER
with ThreadPoolExecutor(max_workers=20) as executor:
    Count = 0
    itera = 1
    futures = []
    session = requests.Session()

    # ITERATE THROUGH ALL LEADERBOARDS
    for i in range(len(Leaderboard_ID)):
        r = session.get(f"https://kovaaks.com/webapp-backend/leaderboard/scores/global?leaderboardId={Leaderboard_ID[i]}&page=0&max=100").json()
        Max_Page = r['total'] // 100

        # ITERATE THROUGH ALL LEADERBOARD PAGES AND SEND TO FUNCTION
        for ii in range(Max_Page + 1):
            futures.append(executor.submit(process_leaderboard, Leaderboard_ID[i], ii, session, itera, Count, score_lock, Score_Dic, RankReq, VoltsReq))

        # LOCK CRITERIA (NEEDED)
        with score_lock:
            Count += 1
            if Count >= 18:
                Count = 0
                itera = 2

    # PROCESS RESULTS
    for future in as_completed(futures):
        future.result()  # No need to handle this since the processing is done within the function

    session.close()

# SORT EASY VOLTS THEN VOLTS
Score_Dic_S = dict(sorted(Score_Dic.items(), key=lambda item: (item[1][18], item[1][39]), reverse=True))

# ITERATE THROUGH ALL KEYS IN DICTIONARY
Count = 0
for key, values in Score_Dic_S.items():
    RankL = values[19:37]

    # CALCULATE RANKS
    for i in range(-1, 11):
        if max(RankL[0:3]) >= i and max(RankL[3:6]) >= i and max(RankL[6:9]) >= i and max(RankL[9:12]) >= i and max(RankL[12:15]) >= i and max(RankL[15:18]) >= i:
            values[37] = Ranks[i+2]
        if min(RankL) >= i and i >= 0:
            values[37] = Ranks[i + 2] + " Complete"

    # COUNT OF RELEVANT ENTRIES
    if values[37] != -2:
        Count += 1
        values[40] = Count

    # CONVERT RANKL TO ACTUAL RANKS (NUMBERS TO NAMES)
    for i in range(len(RankL)):
        RankL[i] = Ranks[RankL[i]+2]

    values[19:37] = RankL

print('test1')
# GOOGLE SHEETS API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
print('test2')
# JSON CREDENTIAL FILE PATH
creds_dict = json.loads(os.getenv('GSPREAD_CREDENTIALS'))
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
print('test3')
# AUTHORIZE THE CLIENT
client = gspread.authorize(creds)
print('test4')
# OPEN GOOGLE SHEET
sheet = client.open('S3_Voltaic').sheet1
print('tes5')
# CLEAR EXISTING DATA IN GOOGLE SHEET
sheet.clear()
print('test6')
# SHEET HEADERS
header = ['PlayerID',  'Pasu Voltaic','B180 Voltaic','Popcorn Voltaic','ww3t Voltaic','1w4ts Voltaic','6 Sphere Hipfire Voltaic',
          'Smoothbot Voltaic', 'Air Angelic 4 Voltaic', 'PGTI Voltaic', 'FuglaaXYZ Voltaic', 'Ground Plaza Voltaic', 'Air Voltaic',
          'patTS Voltaic', 'psalmTS Voltaic', 'voxTS Voltaic', 'kinTS Voltaic', 'B180T Voltaic', 'Smoothbot TS Voltaic',
          'Volts', 'Pasu Voltaic #', 'B180 Voltaic #', 'Popcorn Voltaic #', 'ww3t Voltaic #', '1w4ts Voltaic #', '6 Sphere Hipfire Voltaic #',
          'Smoothbot Voltaic #', 'Air Angelic 4 Voltaic #', 'PGTI Voltaic #', 'FuglaaXYZ Voltaic #', 'Ground Plaza Voltaic #', 'Air Voltaic #',
          'patTS Voltaic #', 'psalmTS Voltaic #', 'voxTS Voltaic #', 'kinTS Voltaic #', 'B180T Voltaic #', 'Smoothbot TS Voltaic #',
          'Rank', 'Player', 'Volts Easy', 'Number', 'Percentage']

# WRITE HEADERS TO FIRST ROW
sheet.append_row(header)
print('test7')
# SEND DATA FROM DICTIONARY TO ARRAY
Per = 0
rows_to_update = []
for key, values in Score_Dic_S.items():
    if values[37] != -2:
        values[41] = round(1 - Per / Count, 6)
        if values[38] is not None:
            values[38] = values[38].encode('ascii', 'ignore').decode('ascii')
        else:
            values[38] = ''
        # Add the row to the list
        rows_to_update.append([key] + values)
        Per += 1

# UPDATE GOOGLE SHEET WITH ALL ARRAY DATA
start_cell = 'A2'
sheet.update(rows_to_update, start_cell)
