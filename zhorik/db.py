#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sqlite3
import datetime as dt
import json

# NULL — значение NULL
# INTEGER — целое число
# REAL — число с плавающей точкой
# TEXT — текст
# BLOB — бинарное представление крупных объектов, хранящееся в точности с тем, как его ввели


class DB( object ):
    db_path = "var/db.sqlite3"
    def __init__(self):

        conn = sqlite3.connect( self.db_path )
        c = conn.cursor()
        q = """CREATE TABLE IF NOT EXISTS pump ( 
            time_utc_unix INTEGER UNIQUE,
            time_utc_text TEXT,
            time_local_text TEXT,
            insulin_unitsRemaining REAL,
            insulin_minutesOfRemaining REAL,
            insulin_active REAL,
            bolus_isBolusingNormal INTEGER,
            bolus_isBolusingSquare INTEGER,
            bolus_isBolusingDual INTEGER,
            bolus_delivered REAL,
            bolus_lastAmount REAL,
            bolus_lastTime_local TEXT,
            bolus_lastReference INTEGER,
            bolus_reference INTEGER,
            bolus_recentWizard INTEGER,
            bolus_wizardBglMg REAL,
            bolus_wizardBglMm REAL,
            bolus_minutesRemaining INTEGER,
            basal_currentBasalRate REAL,
            basal_tempRate REAL,
            basal_tempPercentage REAL,
            basal_tempMinutesRemaining INTEGER,
            basal_activePattern INTEGER,
            basal_activeTempPattern INTEGER,
            basal_unitsDeliveredToday REAL,
            basal_realBasalRate REAL,
            plgm_isAlertOnHigh INTEGER,
            plgm_isAlertOnLow INTEGER,
            plgm_isAlertBeforeHigh INTEGER,
            plgm_isAlertBeforeLow INTEGER,
            plgm_isAlertSuspend INTEGER,
            plgm_isAlertSuspendLow INTEGER,
            alert_alert INTEGER,
            alert_time_local TEXT,
            alert_isSilenceHigh INTEGER,
            alert_isSilenceHighLow INTEGER,
            alert_isSilenceAll INTEGER,
            alert_silenceMinutesRemaining INTEGER,
            pump_isSuspended INTEGER,
            pump_isDeliveringInsulin INTEGER,
            pump_isTempBasalActive INTEGER,
            pump_isCgmActive INTEGER,
            pump_battery INTEGER,
            pump_trendArrow REAL,
            pump_pumpDateTime TEXT,
            pump_serial TEXT,
            sensor_statusTxt TEXT,
            sensor_bglMm REAL,
            sensor_bglMg INTEGER,
            sensor_isStatusCalibrating INTEGER,
            sensor_isStatusCalibrationComplete INTEGER,
            sensor_isStatusException INTEGER,
            sensor_bglTime TEXT,
            sensor_minutesRemaining INTEGER,
            sensor_battery INTEGER,
            sensor_rateOfChange REAL
        );
        CREATE TABLE IF NOT EXISTS sensor (
            sensor_bglTime_unix INTEGER UNIQUE,
            sensor_bglTime TEXT,
            sensor_bglMm REAL,
            sensor_bglMg INTEGER,
            sensor_predicted_bglMm REAL,
            isig REAL,
            source INTEGER,
            sensorError INTEGER,
            sensor_statusTxt TEXT
        );
        CREATE TABLE IF NOT EXISTS history_BOLUS_WIZARD_ESTIMATE (
            event_time_unix INTEGER UNIQUE,
            event_time TEXT,
            bg_input REAL,
            carb_input REAL,
            food_estimate REAL,
            carb_ratio REAL,
            correction_estimate REAL,
            bolus_wizard_estimate REAL,
            estimate_modified_by_user,
            final_estimate REAL,
            active_insulin REAL,
            active_insulin_correction REAL,
            programmed INTEGER,
            low_bg_target REAL,
            high_bg_target REAL,
            bg_units_name TEXT,
            carb_units_name TEXT,
            bolus_step_size_name TEXT,
            bg_units REAL,
            carb_units REAL,
            isf REAL,
            bolus_step_size REAL
        );
        CREATE TABLE IF NOT EXISTS history_DUAL_BOLUS_PART_DELIVERED (
            event_time_unix INTEGER UNIQUE,
            event_time TEXT,
            bolus_source_name  TEXT,
            bolus_source INTEGER,
            bolus_number INTEGER,
            preset_bolus_number_name TEXT,
            normal_programmed_amount REAL, 
            square_programmed_amount REAL,
            programmed_duration INTEGER,
            active_insulin REAL,
            delivered_amount REAL,
            bolus_part_name TEXT,
            bolus_part INTEGER,
            delivered_duration INTEGER, 
            preset_bolus_number INTEGER,
            programmedEvent_time_unix INTEGER,
            canceled INTEGER
        ); 
        CREATE TABLE IF NOT EXISTS history_DUAL_BOLUS_PROGRAMMED (
            event_time_unix INTEGER UNIQUE,
            event_time TEXT,
            bolus_source_name TEXT,
            bolus_source INTEGER,
            bolus_number INTEGER,
            preset_bolus_number_name TEXT,
            normal_programmed_amount REAL,
            square_programmed_amount REAL,
            total_programmed_amount REAL,
            programmed_duration INTEGER,
            active_insulin REAL,
            preset_bolus_number INTEGER,
            bolusWizardEvent_time_unix INTEGER 
        ); 
        CREATE TABLE IF NOT EXISTS history_NORMAL_BOLUS_PROGRAMMED (
            event_time_unix INTEGER UNIQUE,
            event_time TEXT,
            bolus_source_name TEXT,
            bolus_source INTEGER,
            bolus_number INTEGER,
            preset_bolus_number_name TEXT,
            programmed_amount REAL,
            active_insulin REAL,
            preset_bolus_number INTEGER,
            bolusWizardEvent_time_unix INTEGER 
        );
        CREATE TABLE IF NOT EXISTS history_NORMAL_BOLUS_DELIVERED (
            event_time_unix INTEGER UNIQUE,
            event_time TEXT,
            bolus_source_name  TEXT,
            bolus_source INTEGER,
            bolus_number INTEGER,
            preset_bolus_number_name TEXT,
            programmed_amount REAL, 
            active_insulin REAL,
            delivered_amount REAL,
            preset_bolus_number INTEGER,
            programmedEvent_time_unix INTEGER,
            canceled INTEGER
        ); 
        CREATE TABLE IF NOT EXISTS history_SQUARE_BOLUS_PROGRAMMED (
            event_time_unix INTEGER UNIQUE,
            event_time TEXT,
            bolus_source_name TEXT,
            bolus_source INTEGER,
            bolus_number INTEGER,
            preset_bolus_number_name TEXT,
            programmed_amount REAL,
            programmed_duration INTEGER,
            active_insulin REAL,
            preset_bolus_number INTEGER,
            bolusWizardEvent_time_unix INTEGER 
        ); 
        CREATE TABLE IF NOT EXISTS history_SQUARE_BOLUS_DELIVERED (
            event_time_unix INTEGER UNIQUE,
            event_time TEXT,
            bolus_source_name  TEXT,
            bolus_source INTEGER,
            bolus_number INTEGER,
            preset_bolus_number_name TEXT,
            programmed_amount REAL, 
            active_insulin REAL,
            delivered_amount REAL,
            preset_bolus_number INTEGER,
            programmed_duration INTEGER,
            delivered_duration INTEGER,
            programmedEvent_time_unix INTEGER,
            canceled INTEGER
        ); 
        CREATE TABLE IF NOT EXISTS history_DAILY_TOTALS(
            event_time_unix INTEGER UNIQUE,
            event_time TEXT,
            basal_insulin REAL,
            basal_percent INTEGER,
            bg_average INTEGER,
            bolus_insulin REAL,
            bolus_percent INTEGER,
            bolus_wizard_correction_only_bolus_count INTEGER,
            bolus_wizard_food_and_correction_bolus_count INTEGER,
            bolus_wizard_food_only_bolus_count INTEGER,
            bolus_wizard_usage_count INTEGER,
            carb_units INTEGER,
            carb_units_name TEXT,
            date_time TEXT,
            date_time_unix INTEGER,
            duration INTEGER,
            falling_rate_alerts INTEGER,
            high_bg_alerts INTEGER,
            high_manually_entered_bg INTEGER,
            high_meter_bg INTEGER,
            high_predictive_alerts INTEGER,
            lgs_suspension_duration INTEGER,
            low_bg_alerts INTEGER,
            low_glucose_suspend_alerts INTEGER,
            low_manually_entered_bg INTEGER,
            low_meter_bg INTEGER,
            low_predictive_alerts INTEGER,
            manual_bolus_count INTEGER,
            manually_entered_bg_average INTEGER,
            manually_entered_bg_count INTEGER,
            meter_bg_average INTEGER,
            meter_bg_count INTEGER,
            percent_above_high INTEGER,
            percent_below_low INTEGER,
            percent_within_limit INTEGER,
            predictive_low_glucose_suspend_alerts INTEGER,
            rising_rate_alerts INTEGER,
            sg_average INTEGER,
            sg_count INTEGER,
            sg_duration_above_high INTEGER,
            sg_duration_below_low INTEGER,
            sg_duration_within_limit INTEGER,
            sg_stddev INTEGER,
            total_bolus_wizard_insulin_as_correction_only_bolus REAL,
            total_bolus_wizard_insulin_as_food_and_correction REAL,
            total_bolus_wizard_insulin_as_food_only_bolus REAL,
            total_food_input REAL,
            total_insulin REAL,
            total_manual_bolus_insulin REAL


        ); 

         
        """
        c.executescript(q)
        conn.commit()
        conn.close()

    def insert_pump_status(self, status):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "INSERT INTO pump VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
        c.execute(q,status)
        conn.commit()
        conn.close()

    def get_last_record_pump_status(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "SELECT time_utc_unix FROM pump ORDER BY time_utc_unix DESC LIMIT 1"
        c.execute(q)
        result = c.fetchone()
        conn.commit()
        conn.close()
        if result is None:
            return result
        else:
            return result[0]

    def insert_sensor_status(self, sensor):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "INSERT INTO sensor VALUES (?,?,?,?,?,?,?,?,?);"
        c.executemany(q,sensor)
        conn.commit()
        conn.close()

    def get_last_record_sensor_status(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "SELECT sensor_bglTime_unix FROM sensor ORDER BY sensor_bglTime_unix DESC LIMIT 1"
        c.execute(q)
        result = c.fetchone()
        conn.commit()
        conn.close()
        if result is None:
            return result
        else:
            return result[0]

    def insert_history_bolus_wizard_estimate(self, history):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "INSERT INTO history_BOLUS_WIZARD_ESTIMATE VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
        c.executemany(q,history)
        conn.commit()
        conn.close()

    def get_last_record_history_bolus_wizard_estimate(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "SELECT event_time_unix FROM history_BOLUS_WIZARD_ESTIMATE ORDER BY event_time_unix DESC LIMIT 1"
        c.execute(q)
        result = c.fetchone()
        conn.commit()
        conn.close()
        if result is None:
            return result
        else:
            return result[0]

    def insert_history_dual_bolus_part_delivered(self, history):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "INSERT INTO history_DUAL_BOLUS_PART_DELIVERED VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
        c.executemany(q,history)
        conn.commit()
        conn.close()

    def get_last_record_history_dual_bolus_part_delivered(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "SELECT event_time_unix FROM history_DUAL_BOLUS_PART_DELIVERED ORDER BY event_time_unix DESC LIMIT 1"
        c.execute(q)
        result = c.fetchone()
        conn.commit()
        conn.close()
        if result is None:
            return result
        else:
            return result[0]

    def insert_history_dual_bolus_programmed(self, history):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "INSERT INTO history_DUAL_BOLUS_PROGRAMMED VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);"
        c.executemany(q,history)
        conn.commit()
        conn.close()

    def get_last_record_history_dual_bolus_programmed(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "SELECT event_time_unix FROM history_DUAL_BOLUS_PROGRAMMED ORDER BY event_time_unix DESC LIMIT 1"
        c.execute(q)
        result = c.fetchone()
        conn.commit()
        conn.close()
        if result is None:
            return result
        else:
            return result[0]

    def get_last_record_history_normal_bolus_programmed(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "SELECT event_time_unix FROM history_NORMAL_BOLUS_PROGRAMMED ORDER BY event_time_unix DESC LIMIT 1"
        c.execute(q)
        result = c.fetchone()
        conn.commit()
        conn.close()
        if result is None:
            return result
        else:
            return result[0]

    def insert_history_normal_bolus_programmed(self, history):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "INSERT INTO history_NORMAL_BOLUS_PROGRAMMED VALUES (?,?,?,?,?,?,?,?,?,?);"
        c.executemany(q,history)
        conn.commit()
        conn.close()

    def get_last_record_history_normal_bolus_delivered(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "SELECT event_time_unix FROM history_NORMAL_BOLUS_DELIVERED ORDER BY event_time_unix DESC LIMIT 1"
        c.execute(q)
        result = c.fetchone()
        conn.commit()
        conn.close()
        if result is None:
            return result
        else:
            return result[0]

    def insert_history_normal_bolus_delivered(self, history):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "INSERT INTO history_NORMAL_BOLUS_DELIVERED VALUES (?,?,?,?,?,?,?,?,?,?,?,?);"
        c.executemany(q,history)
        conn.commit()
        conn.close()

    def  get_last_record_history_square_bolus_programmed(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "SELECT event_time_unix FROM history_SQUARE_BOLUS_PROGRAMMED ORDER BY event_time_unix DESC LIMIT 1"
        c.execute(q)
        result = c.fetchone()
        conn.commit()
        conn.close()
        if result is None:
            return result
        else:
            return result[0]

    def  get_last_record_history_square_bolus_delivered(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "SELECT event_time_unix FROM history_SQUARE_BOLUS_DELIVERED ORDER BY event_time_unix DESC LIMIT 1"
        c.execute(q)
        result = c.fetchone()
        conn.commit()
        conn.close()
        if result is None:
            return result
        else:
            return result[0]

    def  get_last_record_history_daily_totals(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "SELECT event_time_unix FROM history_DAILY_TOTALS ORDER BY event_time_unix DESC LIMIT 1"
        c.execute(q)
        result = c.fetchone()
        conn.commit()
        conn.close()
        if result is None:
            return result
        else:
            return result[0]

    def insert_history_daily_totals(self, history):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "INSERT INTO history_DAILY_TOTALS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
        c.executemany(q,history)
        conn.commit()
        conn.close()

    def insert_history_square_bolus_programmed(self, history):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "INSERT INTO history_SQUARE_BOLUS_PROGRAMMED VALUES (?,?,?,?,?,?,?,?,?,?,?);"
        c.executemany(q,history)
        conn.commit()
        conn.close()

    def insert_history_square_bolus_delivered(self, history):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "INSERT INTO history_SQUARE_BOLUS_DELIVERED VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
        c.executemany(q,history)
        conn.commit()
        conn.close()

    def get_last_bgl(self, time):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        q = "SELECT sensor_bglMg, sensor_bglTime FROM sensor WHERE sensor_bglTime_unix >= ? AND sensor_bglTime_unix <= ? AND sensor_bglMg != 0 ORDER BY sensor_bglTime_unix DESC LIMIT 1"
        from_time_bg = time - (60 * 60)
        c.execute(q, (from_time_bg, time))
        result_bg = c.fetchall()
        if len(result_bg) == 1:
            bgl = result_bg[0][0]
            time_bgl_tmp = result_bg[0][1].split(" ")[1].split(":")
            time_bgl = "{}:{}".format(time_bgl_tmp[0], time_bgl_tmp[1])
        else:
            bgl = "--"
            time_bgl = "--:--"
        conn.commit()
        conn.close()
        return bgl, time_bgl

    def get_time_of_day(self, time, data):
        if time.hour >= 7 and time.hour < 13:
            data.update({'time_of_day': "breakfast"})
        elif time.hour >= 13 and time.hour < 16:
            data.update({'time_of_day': "lunch"})
        elif time.hour >= 16 and time.hour < 21:
            data.update({'time_of_day': "dinner"})
        else:
            data.update({'time_of_day': "night"})

    def get_bolus_wizard(self, from_utc, to_utc):

        out = []

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        q = "SELECT hBWE.event_time_unix, hBWE.event_time, hBWE.carb_input, hBWE.food_estimate, hBWE.carb_ratio, hBWE.final_estimate, " \
            "hBWE.active_insulin, hBWE.isf, hDBP.bolus_number, hDBP.normal_programmed_amount, " \
            "hDBP.square_programmed_amount, hDBP.total_programmed_amount, hDBP.programmed_duration, " \
            "hDBPD.delivered_amount, hDBPD.bolus_part_name, hDBPD.bolus_part, hDBPD.delivered_duration, hDBPD.canceled, hDBPD.event_time " \
            "FROM history_BOLUS_WIZARD_ESTIMATE hBWE " \
            "JOIN history_DUAL_BOLUS_PROGRAMMED hDBP " \
            "on (hBWE.final_estimate = hDBP.total_programmed_amount AND " \
            "hBWE.programmed = 1 AND hBWE.event_time_unix = hDBP.bolusWizardEvent_time_unix) "\
            "JOIN history_DUAL_BOLUS_PART_DELIVERED hDBPD on hDBP.bolus_number = hDBPD.bolus_number " \
            "WHERE hBWE.event_time_unix >= ? AND hBWE.event_time_unix <= ? " \
            "ORDER BY hBWE.event_time_unix, hDBPD.bolus_part"
            # "AND food_estimate != 0 ORDER BY hBWE.event_time_unix, hDBPD.bolus_part"

        c.execute(q, (from_utc, to_utc))

        dual_bolus = c.fetchall()

        for row in dual_bolus:
            time = dt.datetime.fromtimestamp(row[0])

            data = {
                "time_unix" : row[0],
                "time" : row[1],
                "type" : "dual",
                "time_of_day" : "none",
                "carb_input" : row[2],
                "food_estimate" : row[3],
                "carb_ratio" : row[4],
                "final_estimate" : row[5],
                "active_insulin" : row[6],
                "isf" : row[7],
                "bolus_number" : row[8],
                "normal_programmed_amount" : row[9],
                "square_programmed_amount" : row[10],
                "total_programmed_amount" : row[11],
                "programmed_duration" : row[12],
                "delivered_amount" : row[13],
                "bolus_part_name" : row[14],
                "bolus_part": row[15],
                "delivered_duration" : row[16],
                "canceled" : row[17],
                "event_time" : row[18]
            }

            self.get_time_of_day(time,data)
            out.append(data)

        q = "SELECT hBWE.event_time_unix, hBWE.event_time, hBWE.carb_input, hBWE.food_estimate, hBWE.carb_ratio, hBWE.final_estimate, " \
            "hBWE.active_insulin, hBWE.isf, hNBP.bolus_number, hNBP.programmed_amount, hNBD.delivered_amount, hNBD.canceled, hNBD.event_time " \
            "FROM history_BOLUS_WIZARD_ESTIMATE hBWE " \
            "JOIN history_NORMAL_BOLUS_PROGRAMMED hNBP " \
            "on (hBWE.final_estimate = hNBP.programmed_amount AND " \
            "hBWE.programmed = 1 AND hBWE.event_time_unix = hNBP.bolusWizardEvent_time_unix) "\
            "JOIN history_NORMAL_BOLUS_DELIVERED hNBD on hNBP.bolus_number = hNBD.bolus_number " \
            "WHERE hBWE.event_time_unix >= ? AND hBWE.event_time_unix <= ? " \
            "ORDER BY hBWE.event_time_unix"
            # "AND food_estimate != 0 ORDER BY hBWE.event_time_unix"

        c.execute(q, (from_utc, to_utc))
        normal_bolus = c.fetchall()
        for row in normal_bolus:
            time = dt.datetime.fromtimestamp(row[0])

            data = {
                "time_unix" : row[0],
                "time" : row[1],
                "type" : "normal",
                "time_of_day" : "none",
                "carb_input" : row[2],
                "food_estimate" : row[3],
                "carb_ratio" : row[4],
                "final_estimate" : row[5],
                "active_insulin" : row[6],
                "isf" : row[7],
                "bolus_number" : row[8],
                "programmed_amount" : row[9],
                "delivered_amount" : row[10],
                "canceled" : row[11],
                "event_time" : row[12]
            }
            self.get_time_of_day(time,data)
            out.append(data)

        q = "SELECT hBWE.event_time_unix, hBWE.event_time, hBWE.carb_input, hBWE.food_estimate, hBWE.carb_ratio, hBWE.final_estimate, " \
            "hBWE.active_insulin, hBWE.isf, hSBP.bolus_number, hSBP.programmed_amount, hSBD.delivered_amount, hSBD.delivered_duration, hSBD.canceled, hSBD.event_time " \
            "FROM history_BOLUS_WIZARD_ESTIMATE hBWE " \
            "JOIN history_SQUARE_BOLUS_PROGRAMMED hSBP " \
            "on (hBWE.final_estimate = hSBP.programmed_amount AND " \
            "hBWE.programmed = 1 AND hBWE.event_time_unix = hSBP.bolusWizardEvent_time_unix) "\
            "JOIN history_SQUARE_BOLUS_DELIVERED hSBD on hSBP.bolus_source = hSBD.bolus_source " \
            "WHERE hBWE.event_time_unix >= ? AND hBWE.event_time_unix <= ? " \
            "ORDER BY hBWE.event_time_unix"
            # "AND food_estimate != 0 ORDER BY hBWE.event_time_unix"

        c.execute(q, (from_utc, to_utc))
        square_bolus = c.fetchall()
        for row in square_bolus:
            time = dt.datetime.fromtimestamp(row[0])

            data = {
                "time_unix" : row[0],
                "time" : row[1],
                "type" : "square",
                "time_of_day" : "none",
                "carb_input" : row[2],
                "food_estimate" : row[3],
                "carb_ratio" : row[4],
                "final_estimate" : row[5],
                "active_insulin" : row[6],
                "isf" : row[7],
                "bolus_number" : row[8],
                "programmed_amount" : row[9],
                "delivered_amount" : row[10],
                "delivered_duration" : row[11],
                "canceled" : row[12],
                "event_time": row[13]
            }
            self.get_time_of_day(time,data)
            out.append(data)

        conn.commit()
        conn.close()
        return out

    def dict_factory(self, cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def get_history_daily_totals(self, from_utc, to_utc):
        out = {}

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = self.dict_factory
        c = conn.cursor()

        q = "SELECT hDT.basal_insulin, hDT.basal_percent, "\
            "hDT.bolus_insulin, hDT.bolus_percent, hDT.total_insulin, " \
            "hDT.low_meter_bg, hDT.high_meter_bg, hDT.sg_average, " \
            "hDT.total_food_input, hDT.meter_bg_count, hDT.total_bolus_wizard_insulin_as_food_only_bolus " \
            "FROM history_DAILY_TOTALS hDT " \
            "WHERE hDT.event_time_unix >= ? AND hDT.event_time_unix <= ? " \
            "ORDER BY event_time_unix DESC LIMIT 1"

        c.execute(q, (from_utc, to_utc))

        daily_totals = c.fetchall()
        if len(daily_totals) == 1:
            out = daily_totals[0]

        conn.commit()
        conn.close()
        return out
