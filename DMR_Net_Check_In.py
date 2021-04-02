#
#  This script generates a weekly report from our DMR Net Check-In data.
#
#

import pandas as pd
#import odf
import os
import sys
#import logzero


# setup output parameters for jupyter interactive execution
# Use 3 decimal places in output display
pd.set_option("display.precision", 0)

# Don't wrap repr(DataFrame) across additional lines
pd.set_option("display.expand_frame_repr", False)

# Set max rows displayed in output to 25
pd.set_option("display.max_rows", 150)


# point to our various input files - adjust this to your environment
data_dir = '/home/ed/syncthing/ham-radio/ARES-RACES/Nets--PNW_ARES_DMR_Weekly_Net'
filename = 'Check_In_Data.xlsx'
data_file = os.path.join(data_dir, filename)
output_file = os.path.join(data_dir, "PNW_Digital_ARES_EMCOMM_Weekly_Net_Check_In_Form.txt")

# Our check-in form as a dictionary
blank_checkin_form_dict = {
      'ID': { '1':{'Boundary':[],'Bonner':[],'Kootenai':[],'Benewah':[],'Shoshone':[]},
              '2':{'Latah':[],'Nez Pierce':[],'Lewis':[],'Clearwater':[],'Idaho':[]},
              '3':{'Adams':[],'Washington':[],'Payette':[],'Gem':[],'Canyon':[],'Ada':[],
                   'Owyhee':[],'Valley':[],'Boise':[],'Elmore':[]},
              '4':{'Camas':[],'Blaine':[],'Gooding':[],'Lincoln':[],'Jerome':[],'Twin Falls':[],
                   'Minidoka':[],'Cassia':[]},
              '5':{'Bingham':[],'Power':[],'Bannock':[],'Oneida':[],'Franklin':[],
                   'Bear Lake':[],'Caribou':[]},
              '6':{'Lemhi':[],'Custer':[],'Butte':[],'Clark':[],'Jefferson':[],'Fremont':[],
                   'Madison':[],'Teton':[],'Bonneville':[]}
            },
      'OR': { '1':{'Clatsop':[],'Columbia':[],'Tillamook':[],'Washington':[],'Multnomah':[],
                   'Clackamas':[]},
              '2':{'Hood River':[],'Wasco':[],'Sherman':[],'Jefferson':[],'Deschutes':[],
                   'Crook':[]},
              '3':{'Gilliam':[],'Wheeler':[],'Morrow':[],'Umatilla':[],'Union':[],
                   'Wallowa':[]},
              '4':{'Yamhill':[],'Polk':[],'Lincoln':[],'Benton':[],'Marion':[],
                   'Linn':[],'Lane':[]},
              '5':{'Douglas':[],'Coos':[],'Curry':[],'Josephine':[],
                   'Jackson':[]},
              '6':{'Klamath':[],'Lake':[],'Harney':[],'Grant':[],'Baker':[],
                   'Malheur':[]}
            },
    'WA': {   '1':{'Island':[], 'San Juan':[], 'Skagit':[], 'Snohomish':[], 'Whatcom':[], 'unknown':[]},
              '2':{'Clallam':[],'Jefferson':[],'Kitsap':[], 'unknown':[]},
              '3':{'Grays Harbor':[],'Lewis':[],'Mason':[],'Pacific':[],'Thurston':[]},
              '4':{'Clark':[],'Cowlitz':[],'Skamania':[],'Wahkiakum':[]},
              '5':{'Pierce':[],'East Thurston':[]},
              '6':{'King':[]},
              '7':{'Chelan':[],'Douglas':[],'Grant':[],'Kittitas':[],'Okanogan':[]},
              '8':{'Benton':[],'Franklin':[],'Klickitat':[],'Walla Walla':[],'Yakima':[]},
              '9':{'Adams':[],'Asotin':[],'Columbia':[],'Ferry':[],'Garfield':[],'Lincoln':[],
                   'Pend Orielle':[],'Spokane':[],'Stevens':[],'Whitman':[]},
              'State EMD':{'none':[]}
            },
       'Philippines': { '-':{'-':[]}
            },
       'Canada': {'n/a':{'n/a':[]}
            },
       'Visitor' : { '-':{'-':[]}
            }
        }


# create dataframes from input file
print("Reading input file from: ")
print("  ", data_file)
xls = pd.ExcelFile(data_file)
checkins_df = pd.read_excel(xls, "Check-ins", dtype=str)
checkins_df["Date"] = pd.to_datetime(checkins_df["Date"]).dt.strftime('%Y-%m-%d')
#checkins_df['Date'] = checkins_df['Date'].strftime('%Y-%m-%d')
#print("checkins_df:")
#print(checkins_df)
callinfo_df = pd.read_excel(xls, "Call_Data", dtype=str)
#print("callinfo_df:")
#print(callinfo_df)
hh_dir_df = pd.read_excel(xls, "Hamshack_Hotline", dtype=str)
#print("hh_dir_df:")
#print(hh_dir_df)

# build dictionary of HH phone numbers
hh_phone_dict = {"Callsign" : "HH Number"}
for i,row in hh_dir_df.iterrows():
    callsign = row[1]
    hh_num = row[7]
    # we only add the hh number first seen for a unique callsign
    if callsign not in hh_phone_dict.keys():
        hh_phone_dict.update({callsign : hh_num})

# fill in any missing data (NAN's) with default text
callinfo_df['Name'].fillna('', inplace=True)
callinfo_df['State'].fillna('none', inplace=True)
callinfo_df['District'].fillna('unknown', inplace=True)
callinfo_df['County'].fillna('unknown', inplace=True)
callinfo_df['Affiliation'].fillna('', inplace=True)

# build dictionary of calls indexed by check-in date
calls_on_date_dict = {}
checkin_count_dict = {"Callsign" : "Check-in Count"}
for i,row in checkins_df.iterrows():
    checkin_date = row[0]
    checkin_call = row[1]
    if checkin_date in calls_on_date_dict.keys():
        if checkin_call not in calls_on_date_dict[checkin_date]:
            calls_on_date_dict[checkin_date].append(checkin_call)
    else:
        calls_on_date_dict.update({checkin_date:[checkin_call]})
    if checkin_call in checkin_count_dict.keys():
        checkin_count_dict[checkin_call] = checkin_count_dict[checkin_call] + 1
    else:
        checkin_count_dict.update({checkin_call : 1})

# build dictionary of call info indexed by callsign
call_data_dict = {}
for i,row in callinfo_df.iterrows():
    # row of data is call, name, state, district, county, affiliation, hh_num
    # dictionary becomes: { call: [name[0], state[1], district[2], county[3], affiliation[4]], hh_num[5]}
    callsign = row[0]
    if callsign in hh_phone_dict.keys():
        hh_num = hh_phone_dict[callsign]
    else:
        hh_num = ""
    call_data_dict.update({callsign:[row[1],row[2],row[3],row[4],row[5],hh_num]})

# build checkin_form_dict dictionary
checkin_form_dict = blank_checkin_form_dict
for call in call_data_dict.keys():
    #print("Looking at:", call)
    #if call in hh_phone_dict.keys():
        #print("Call "+call+" has HH VOIP #"+hh_phone_dict[call])
    call_state = call_data_dict[call][1]
    call_dist = call_data_dict[call][2]
    call_county = call_data_dict[call][3]
    call_affil = call_data_dict[call][4]
    call_hh_num = call_data_dict[call][5]
    #print("State, Dist, Cnty, Affil, HH Num: ", call_state, call_dist, call_county, call_affil, call_hh_num)

    # sanity check the data - error if not found in dictionary keys...
    #print("Verifying at: ", call, call_state, call_dist, call_county)
    if call_state not in checkin_form_dict.keys():
        print("ERROR: ",call_state,"not in checkin_form_dict!")
    if call_dist not in checkin_form_dict[call_state].keys():
        print("ERROR: ",call_dist, "not in checkin_form_dict under state of", call_state,"!")
    if call_county not in checkin_form_dict[call_state][call_dist].keys():
        print("ERROR: ",call_county, "not in checkin_form_dict under district ", call_dist,"!")

    # okay, safe to move on...
    call_list = checkin_form_dict[call_state][call_dist][call_county]
    #print("   calls in",call_county,":", call_list)
    if call not in call_list:
        checkin_form_dict[call_state][call_dist][call_county].append(call)



# print check-in form based on historical check-ins
with open(output_file,"w") as outfile:
    outfile.write("\n")
    outfile.write("PNW Digital ARES & EMCOMM Check-In Net -  Check-ins for _______________\n\n")
    for state in checkin_form_dict.keys():
        if state not in ['Philippines','Canada','Visitor']:
            outfile.write("State:  " + state +"\n")
            for district in checkin_form_dict[state].keys():
                outfile.write("   District:  " + district + "\n")
                if district not in ["State EMD"]:

                    # print the list of counties in this district
                    outfile.write("      Counties:  ")
                    county_list = sorted(checkin_form_dict[state][district])
                    county_count = len(county_list)
                    index = 0
                    if county_count >= 1:
                        outfile.write(county_list[index])
                        county_count -= 1
                        index += 1
                    while county_count >= 1:
                        if county_list[index] == "unknown":
                            county_count -= 1
                            index += 1
                            continue

                        if (index % 5) == 0:
                            outfile.write("\n         "+county_list[index])
                        else:
                            outfile.write(", "+county_list[index])
                        county_count -= 1
                        index += 1
                    outfile.write("\n\n")

                    # now collect all of the calls from all counties
                    call_list = []
                    for county in sorted(checkin_form_dict[state][district]):
                        for county_call_item in checkin_form_dict[state][district][county]:
                            call_list.append(county_call_item)
                    call_list.sort()
                    for call in call_list:
                        call_name = call_data_dict[call][0]
                        call_county = call_data_dict[call][3]
                        call_affil = call_data_dict[call][4]
                        if call in checkin_count_dict.keys():
                            checkin_count = checkin_count_dict[call]
                            checkin_str = ", [{}]".format(checkin_count)
                        else:
                            checkin_count = 0
                            checkin_str = ""
                        outfile.write("         "+call+", "+call_name+checkin_str)
                        if call in hh_phone_dict.keys():
                            outfile.write(", HH VOIP #"+hh_phone_dict[call])
                        if call_affil != "":
                            outfile.write(", "+call_affil+"\n")
                        else:
                            outfile.write("\n")
                else:
                    for county in checkin_form_dict[state][district].keys():
                        call_list = checkin_form_dict[state][district][county]
                        call_list.sort()
                        for call in call_list:
                            if call in checkin_count_dict.keys():
                                checkin_count = checkin_count_dict[call]
                                checkin_str = ", [{}]".format(checkin_count)
                            else:
                                checkin_count = 0
                                checkin_str = ""
                            outfile.write("      "+call+", "+call_data_dict[call][0]+checkin_str)
                            if call in hh_phone_dict.keys():
                                outfile.write(", HH VOIP #"+hh_phone_dict[call])
                            affil = call_data_dict[call][4]
                            if affil != '':
                                outfile.write(", "+affil+"\n")
                            else:
                                outfile.write("\n")
                outfile.write("\n")
            outfile.write("\n")
        else:
            if state == "Visitor":
                outfile.write("Visitor Check-ins:\n")
            else:
                outfile.write("Country:  "+state+"\n")
            for district in checkin_form_dict[state].keys():
                for county in checkin_form_dict[state][district].keys():
                    call_list = checkin_form_dict[state][district][county]
                    call_list.sort()
                    for call in call_list:
                        if call in checkin_count_dict.keys():
                            checkin_count = checkin_count_dict[call]
                            checkin_str = ", [{}]".format(checkin_count)
                        else:
                            checkin_count = 0
                            checkin_str = ""
                        outfile.write("      "+call+", "+call_data_dict[call][0]+checkin_str)
                        if call in hh_phone_dict.keys():
                            outfile.write(", HH VOIP #"+hh_phone_dict[call])
                        affil = call_data_dict[call][4]
                        if affil != '':
                            outfile.write(", "+affil+"\n")
                        else:
                            outfile.write("\n")
                outfile.write("\n")
            outfile.write("\n")

outfile.close()


# generate net reports based on check-in data
report_dir = os.path.join(data_dir, "reports")
net_day_list = checkins_df['Date'].unique()
for net_day in net_day_list:

    #print("Looking at checkins for: ", net_day)

    # get list of calls checked in on this day
    call_list = calls_on_date_dict[net_day]
    num_checkins = len(call_list)
    #print("  ",num_checkins,"Check-ins on this day: ",call_list)

    # fill in report dictionary from call list
    report_dict = {}
    for call in call_list:

        if call not in call_data_dict.keys():
            print("Unknown call in call database:  {}".format(call))
            continue

        #print("report_dict:")
        #print(report_dict)
        #print("")
        #print("Looking at:", call)
        call_state = call_data_dict[call][1]
        call_dist = call_data_dict[call][2]
        call_county = call_data_dict[call][3]
        call_affil = call_data_dict[call][4]
        #print("   State, Dist, Cnty, Affil: ", call_state, call_dist, call_county, call_affil)

        # sanity check the data - error if not found in dictionary keys...
        #print("Verifying at: ", call, call_state, call_dist, call_county)
        if call_state not in report_dict.keys():
            # make new state entry
            report_dict.update({call_state:{call_dist:{call_county:[call]}}})
            #print("ERROR: ",call_state,"not in report_dict!")
            continue
        if call_dist not in report_dict[call_state].keys():
            report_dict[call_state].update({call_dist:{call_county:[call]}})
            #print("ERROR: ",call_dist, "not in report_dict under state of", call_state,"!")
            continue
        if call_county not in report_dict[call_state][call_dist].keys():
            report_dict[call_state][call_dist].update({call_county:[call]})
            #print("ERROR: ",call_county, "not in report_dict under district ", call_dist,"!")
            continue

        # okay, safe to move on...
        report_call_list = report_dict[call_state][call_dist][call_county]
        #print("   calls in",call_county,":", call_list)
        if call not in report_call_list:
            report_dict[call_state][call_dist][call_county].append(call)

    # Now print a weekly report for this net_day

    output_file = os.path.join(report_dir, "Weekly_Check-in_Report_for_"+net_day+".txt")
    #print("Weekly report for",net_day,"will be written here:")
    #print("   "+output_file)
    with open(output_file,"w") as outfile:

        # Opening text
        outfile.write("\n                      ")
        outfile.write("PNW Digital ARES & EMCOMM Check-In Net - "+str(num_checkins)+" check-ins on "+net_day+"\n")
        outfile.write("\n")
        outfile.write("We had a total of "+str(num_checkins)+" check-ins on "+net_day+" to the Pacific Northwest (PNW)\n")
        outfile.write("Digital ARES & EMCOMM Check-In Net.  Below is the detailed check-in list grouped\n")
        outfile.write("by ARRL ARES districts.  If any of the info (i.e. name or agency affiliation) below is\n")
        outfile.write("incomplete or incorrect, please call me on Hamshack Hotline (HH) VOIP x11893, or\n")
        outfile.write("e-mail \"ec@etc-ares.org\" with the correction(s).\n\n")

        # Check-in details
        for state in sorted(report_dict):
            if state not in ['Philippines','Canada','Visitor']:
                outfile.write(state+" State:\n")
                for district in sorted(report_dict[state]):
                    if district not in ["State EMD"]:
                        outfile.write("   District:  " + district + "\n")
                        for county in sorted(report_dict[state][district]):
                            cur_call_list = report_dict[state][district][county]
                            if len(cur_call_list) > 0:
                                outfile.write("      " + county + " County:\n")
                                cur_call_list.sort()
                                for call in cur_call_list:
                                    affil = call_data_dict[call][4]
                                    hh_num = call_data_dict[call][5]
                                    outfile.write("            "+call+", "+call_data_dict[call][0])
                                    if hh_num != '':
                                        outfile.write(", HH VOIP #"+hh_num)
                                    if affil != '':
                                        outfile.write(", "+affil+"\n")
                                    else:
                                        outfile.write("\n")
                    else:
                        outfile.write("   "+district+"\n")
                        for county in sorted(report_dict[state][district]):
                            cur_call_list = report_dict[state][district][county]
                            cur_call_list.sort()
                            for call in cur_call_list:
                                affil = call_data_dict[call][4]
                                hh_num = call_data_dict[call][5]
                                outfile.write("      "+call+", "+call_data_dict[call][0])
                                if hh_num != '':
                                    outfile.write(", HH VOIP #"+hh_num)
                                if affil != '':
                                    outfile.write(", "+affil+"\n")
                                else:
                                    outfile.write("\n")
                    #outfile.write("\n")
                outfile.write("\n")
        for state in sorted(report_dict):
            if state in ['Canada','Philippines','Visitor']:
                outfile.write(state+":\n")
                for district in sorted(report_dict[state]):
                    for county in sorted(report_dict[state][district]):
                        cur_call_list = report_dict[state][district][county]
                        cur_call_list.sort()
                        for call in cur_call_list:
                            affil = call_data_dict[call][4]
                            hh_num = call_data_dict[call][5]
                            outfile.write("      "+call+", "+call_data_dict[call][0])
                            if hh_num != '':
                                outfile.write(", HH VOIP #"+hh_num)
                            if affil != '':
                                outfile.write(", "+affil+"\n")
                            else:
                                outfile.write("\n")
                    outfile.write("\n")
                outfile.write("\n")

        # output the footer text...
        outfile.write("\nAbout the net:\n\n")
        outfile.write("The PNW Digital ARES & EMCOMM Check-In Net is held every Sunday evening at 6:30 PM local\n")
        outfile.write("time on PNW Regional, DMR talk group 31771 (available on both the PNW Digital http://pnwdigital.net\n")
        outfile.write("and Brandmeister DMR networks).  Anyone interested in Amateur Radio Emergency Communications is\n")
        outfile.write("welcome to check-in.  The net is an opportunity for DMR-capable ARES and EMCOMM hams to exercise\n")
        outfile.write("their DMR equipment in a regional directed net.  The net demonstrates the wide coverage area and\n")
        outfile.write("capability of DMR repeaters and hot spots in our Pacific Northwest region.  It also highlights the\n")
        outfile.write("wide range of EMCOMM-related organizations who have members with DMR capability.\n\n")

        # close this week's report file
        outfile.close()

print("Processed {} check-ins for {}.".format(num_checkins, net_day))
print("All Done!")


# In[ ]:




