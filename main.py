#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging
logging.basicConfig(format='%(asctime)s %(levelname)s [%(name)s] %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


import driver.cnl24lib as cnl24lib
import zhorik.db as db
import zhorik.diary as diary
import binascii
import datetime as dt
import time
import pytz
import pickle
import sys

# Log Level:
# CRITICAL
# ERROR
# WARNING
# INFO
# DEBUG



def get_pump_data(history_day_ago=1):
    mt = cnl24lib.Medtronic600SeriesDriver()
    db_connect = db.DB()

    if mt.open_device():
        logger.debug("Open USB")
        try:
            mt.request_device_info()
            logger.debug("CNL Device serial: {0}".format(mt.device_serial))
            logger.debug("CNL Device model: {0}".format(mt.device_model))
            logger.debug("CNL Device sn: {0}".format(mt.device_sn))
            mt.enter_control_mode()
            try:
                mt.enter_passthrough_mode()
                try:
                    mt.open_connection()
                    try:
                        mt.request_read_info()
                        mt.read_link_key()

                        logger.debug("pump_mac: 0x{0:X}".format(mt.session.pump_mac))
                        logger.debug("link_mac: 0x{0:X}".format(mt.session.link_mac))
                        logger.info("encryption key from pump: {0}".format(binascii.hexlify(mt.session.key)))

                        if mt.negotiate_channel():
                            logger.debug("Channel: 0x{0:X}".format(mt.session.radio_channel))
                            logger.debug("Channel RSSI Perc: {0}%".format(mt.session.radio_rssi_percent))

                            mt.begin_ehsm()
                            try:

                                mt.get_pump_time()
                                now_time = dt.datetime.now()

                                logger.info("Pump time       : {0}".format(mt.pump_time))
                                logger.info("Zhorik time     : {0}".format(now_time))
                                logger.info("Pump time drift : {0}".format(mt.pump_time_drift))

                                real_time_utc = int(time.mktime(mt.pump_time.timetuple()))
                                # Current time (str)
                                real_time_str_local = dt.datetime.fromtimestamp(real_time_utc, local_timezone).\
                                    strftime("%d-%m-%Y %H:%M:%S")

                                real_time_str_utc = dt.datetime.fromtimestamp(real_time_utc, tz=pytz.utc).\
                                    strftime("%d-%m-%Y %H:%M:%S")

                                logger.info("Unix time utc   : {0}".format(real_time_utc))
                                logger.info("Real time utc   : {0}".format(real_time_str_utc))
                                logger.info("Real time local : {0}".format(real_time_str_local))

                                start_date = dt.datetime.now() - dt.timedelta(days=history_day_ago)
                                # # start_date = datetime.datetime.now() - datetime.timedelta(minutes=30)
                                end_date = dt.datetime.max

                                history_type = cnl24lib.HistoryDataType.PUMP_DATA
                                history_info = mt.get_pump_history_info(start_date, end_date, history_type)

                                logger.debug("ReadHistoryInfo Start : {0}".format(history_info.from_date))
                                logger.debug("ReadHistoryInfo End   : {0}".format(history_info.to_date))
                                logger.debug("ReadHistoryInfo Size  : {0}".format(history_info.length))
                                logger.debug("ReadHistoryInfo Block : {0}".format(history_info.blocks))

                                history_pages = mt.get_pump_history(start_date, end_date, history_type)

                                # Dump 'history_pages'
                                with open('history_data.dat', 'wb') as output:
                                    pickle.dump(history_pages, output)
                                    logger.info("History Block Saved to disk")

                                start_date = dt.datetime.now() - dt.timedelta(days=history_day_ago)
                                # start_date = dt.datetime.now() - dt.timedelta(minutes=3)
                                # # end_date = datetime.datetime.now() - datetime.timedelta(days=70)
                                end_date = dt.datetime.max

                                history_type = cnl24lib.HistoryDataType.SENSOR_DATA
                                history_info = mt.get_pump_history_info(start_date, end_date, history_type)

                                logger.debug("ReadHistoryInfo Start : {0}".format(history_info.from_date))
                                logger.debug("ReadHistoryInfo End   : {0}".format(history_info.to_date))
                                logger.debug("ReadHistoryInfo Size  : {0}".format(history_info.length))
                                logger.debug("ReadHistoryInfo Block : {0}".format(history_info.blocks))

                                history_pages = mt.get_pump_history(start_date, end_date, history_type)

                                # Dump 'sensor_pages'
                                with open('sensor_data.dat', 'wb') as output:
                                    pickle.dump(history_pages, output)
                                    logger.info("Sensor Block Saved to disk")

                                events = mt.process_pump_history(history_pages, cnl24lib.HistoryDataType.SENSOR_DATA)
                                all_ev = []

                                # Insert to DB
                                last_time_db_record = db_connect.get_last_record_sensor_status()
                                for ev in events:
                                    if ev.event_type == cnl24lib.NGPHistoryEvent.\
                                            EVENT_TYPE.GENERATED_SENSOR_GLUCOSE_READINGS_EXTENDED_ITEM:
                                        event_time_local = ev.timestamp.strftime("%d-%m-%Y %H:%M:%S")
                                        event_time_utc = int(time.mktime(ev.timestamp.timetuple()))

                                        if last_time_db_record is None or last_time_db_record < event_time_utc:
                                            logger.debug("Insert the record DB(sensor)")
                                            history_sensor = (
                                                event_time_utc,
                                                event_time_local,
                                                ev.sg,
                                                round((ev.sg / cnl24lib.NGPConstants.BG_UNITS.MMOLXLFACTOR), 1),
                                                ev.predictedSg,
                                                ev.isig,
                                                ev.source,
                                                ev.sensorError,
                                                ev.sensorExceptionText
                                            )
                                            all_ev.append(history_sensor)

                                if len(all_ev) > 0:
                                    logger.info("Insert the record(s): {0} to DB(sensor)".format(len(all_ev)))
                                    db_connect.insert_sensor_status(all_ev)

                            finally:
                                mt.finish_ehsm()
                        else:
                            logger.error("Cannot connect to the pump.")
                    finally:
                        mt.close_connection()
                finally:
                    mt.exit_passthrough_mode()
            finally:
                mt.exit_control_mode()
        finally:
            mt.close_device()
    else:
        logger.info("Error open USB")

    return mt


def parsing_history():
    db_connect = db.DB()
    mt = cnl24lib.Medtronic600SeriesDriver()

    # History sensor
    with open('sensor_data.dat', 'rb') as input_file:
        history_pages = pickle.load(input_file)

    events = mt.process_pump_history(history_pages, cnl24lib.HistoryDataType.SENSOR_DATA)
    all_ev = []

    # Insert to DB
    last_time_db_record = db_connect.get_last_record_sensor_status()
    for ev in events:
        if ev.event_type == cnl24lib.NGPHistoryEvent. \
                EVENT_TYPE.GENERATED_SENSOR_GLUCOSE_READINGS_EXTENDED_ITEM:
            event_time_local = ev.timestamp.strftime("%d-%m-%Y %H:%M:%S")
            event_time_utc = int(time.mktime(ev.timestamp.timetuple()))

            if last_time_db_record is None or last_time_db_record < event_time_utc:
                logger.debug("Insert the record DB(sensor)")
                history_sensor = (
                    event_time_utc,
                    event_time_local,
                    ev.sg,
                    round((ev.sg / cnl24lib.NGPConstants.BG_UNITS.MMOLXLFACTOR), 1),
                    ev.predictedSg,
                    ev.isig,
                    ev.source,
                    ev.sensorError,
                    ev.sensorExceptionText
                )
                all_ev.append(history_sensor)

    if len(all_ev) > 0:
        logger.info("Insert(s): {0} to DB(sensor)".format(len(all_ev)))
        db_connect.insert_sensor_status(all_ev)

    # History pump
    last_record_history_bolus_wizard_estimate = db_connect.get_last_record_history_bolus_wizard_estimate()
    last_record_history_dual_bolus_part_delivered = db_connect.get_last_record_history_dual_bolus_part_delivered()
    last_record_history_dual_bolus_programmed = db_connect.get_last_record_history_dual_bolus_programmed()
    last_record_history_normal_bolus_programmed = db_connect.get_last_record_history_normal_bolus_programmed()
    last_record_history_normal_bolus_delivered = db_connect.get_last_record_history_normal_bolus_delivered()
    last_record_history_square_bolus_programmed = db_connect.get_last_record_history_square_bolus_programmed()
    last_record_history_square_bolus_delivered = db_connect.get_last_record_history_square_bolus_delivered()
    last_record_history_daily_totals = db_connect.get_last_record_history_daily_totals()

    with open('history_data.dat', 'rb') as input_file:
        history_pages = pickle.load(input_file)

    events = mt.process_pump_history(history_pages, cnl24lib.HistoryDataType.PUMP_DATA)

    all_daily_totals = []
    all_history_bolus_wizard_estimate = []
    all_history_dual_bolus_part_delivered = []
    all_history_dual_bolus_programmed = []
    all_history_normal_bolus_programmed = []
    all_history_normal_bolus_delivered = []
    all_history_square_bolus_programmed = []
    all_history_square_bolus_delivered = []
    for ev in events:


        if ev.event_type != cnl24lib.NGPHistoryEvent.EVENT_TYPE.PLGM_CONTROLLER_STATE:

            print(ev)

            event_time_local = ev.timestamp.strftime("%d-%m-%Y %H:%M:%S")
            event_time_utc = int(time.mktime(ev.timestamp.timetuple()))

            if ev.event_type == cnl24lib.NGPHistoryEvent.EVENT_TYPE.DAILY_TOTALS:
                if last_record_history_daily_totals is None or last_record_history_daily_totals < event_time_utc:
                    daily_totals = (
                        event_time_utc,
                        event_time_local,
                        ev.basal_insulin,
                        ev.basal_percent,
                        ev.bg_average,
                        ev.bolus_insulin,
                        ev.bolus_percent,
                        ev.bolus_wizard_correction_only_bolus_count,
                        ev.bolus_wizard_food_and_correction_bolus_count,
                        ev.bolus_wizard_food_only_bolus_count,
                        ev.bolus_wizard_usage_count,
                        ev.carb_units,
                        ev.carb_units_name,
                        ev.date.strftime("%d-%m-%Y %H:%M:%S"),
                        int(time.mktime(ev.date.timetuple())),
                        ev.duration,
                        ev.falling_rate_alerts,
                        ev.high_bg_alerts,
                        ev.high_manually_entered_bg,
                        ev.high_meter_bg,
                        ev.high_predictive_alerts,
                        ev.lgs_suspension_duration,
                        ev.low_bg_alerts,
                        ev.low_glucose_suspend_alerts,
                        ev.low_manually_entered_bg,
                        ev.low_meter_bg,
                        ev.low_predictive_alerts,
                        ev.manual_bolus_count,
                        ev.manually_entered_bg_average,
                        ev.manually_entered_bg_count,
                        ev.meter_bg_average,
                        ev.meter_bg_count,
                        ev.percent_above_high,
                        ev.percent_below_low,
                        ev.percent_within_limit,
                        ev.predictive_low_glucose_suspend_alerts,
                        ev.rising_rate_alerts,
                        ev.sg_average,
                        ev.sg_count,
                        ev.sg_duration_above_high,
                        ev.sg_duration_below_low,
                        ev.sg_duration_within_limit,
                        ev.sg_stddev,
                        ev.total_bolus_wizard_insulin_as_correction_only_bolus,
                        ev.total_bolus_wizard_insulin_as_food_and_correction,
                        ev.total_bolus_wizard_insulin_as_food_only_bolus,
                        ev.total_food_input,
                        ev.total_insulin,
                        ev.total_manual_bolus_insulin,
                    )
                    all_daily_totals.append(daily_totals)

            if ev.event_type == cnl24lib.NGPHistoryEvent.EVENT_TYPE.NORMAL_BOLUS_PROGRAMMED:
                if last_record_history_normal_bolus_programmed is None or \
                        last_record_history_normal_bolus_programmed < event_time_utc:
                    # logger.info("Insert the record DB (NORMAL_BOLUS_PROGRAMMED)")
                    if hasattr(ev, 'bolusWizardEvent'):
                        bolus_wizard_event_time_unix = int(time.mktime(ev.bolusWizardEvent.timestamp.timetuple()))
                    else:
                        bolus_wizard_event_time_unix = 0

                    history_normal_bolus_programmed = (
                        event_time_utc,
                        event_time_local,
                        ev.bolus_source_name,
                        ev.bolus_source,
                        ev.bolus_number,
                        ev.preset_bolus_number_name,
                        ev.programmed_amount,
                        ev.active_insulin,
                        ev.preset_bolus_number,
                        bolus_wizard_event_time_unix
                    )
                    all_history_normal_bolus_programmed.append(history_normal_bolus_programmed)

            if ev.event_type == cnl24lib.NGPHistoryEvent.EVENT_TYPE.NORMAL_BOLUS_DELIVERED:
                if last_record_history_normal_bolus_delivered is None or \
                        last_record_history_normal_bolus_delivered < event_time_utc:
                    # logger.info("Insert the record DB (NORMAL_BOLUS_DELIVERED)")
                    if hasattr(ev, 'programmedEvent'):
                        programmed_event_time_unix = int(time.mktime(ev.programmedEvent.timestamp.timetuple()))
                    else:
                        programmed_event_time_unix = 0

                    history_normal_bolus_delivered = (
                        event_time_utc,
                        event_time_local,
                        ev.bolus_source_name,
                        ev.bolus_source,
                        ev.bolus_number,
                        ev.preset_bolus_number_name,
                        ev.programmed_amount,
                        ev.active_insulin,
                        ev.delivered_amount,
                        ev.preset_bolus_number,
                        programmed_event_time_unix,
                        ev.canceled
                    )
                    all_history_normal_bolus_delivered.append(history_normal_bolus_delivered)

            if ev.event_type == cnl24lib.NGPHistoryEvent.EVENT_TYPE.BOLUS_WIZARD_ESTIMATE:
                if last_record_history_bolus_wizard_estimate is None or \
                        last_record_history_bolus_wizard_estimate < event_time_utc:
                    # logger.info("Insert the record DB (BOLUS_WIZARD_ESTIMATE)")

                    history_bolus_wizard_estimate = (
                        event_time_utc,
                        event_time_local,
                        ev.bg_input,
                        ev.carb_input,
                        ev.food_estimate,
                        ev.carb_ratio,
                        ev.correction_estimate,
                        ev.bolus_wizard_estimate,
                        ev.estimate_modified_by_user,
                        ev.final_estimate,
                        ev.active_insulin,
                        ev.active_insulin_correction,
                        ev.programmed,
                        ev.low_bg_target,
                        ev.high_bg_target,
                        ev.bg_units_name,
                        ev.carb_units_name,
                        ev.bolus_step_size_name,
                        ev.bg_units,
                        ev.carb_units,
                        ev.isf,
                        ev.bolus_step_size
                    )
                    all_history_bolus_wizard_estimate.append(history_bolus_wizard_estimate)

            if ev.event_type == cnl24lib.NGPHistoryEvent.EVENT_TYPE.DUAL_BOLUS_PART_DELIVERED:
                if last_record_history_dual_bolus_part_delivered is None or \
                        last_record_history_dual_bolus_part_delivered < event_time_utc:
                    # logger.info("Insert the record DB (DUAL_BOLUS_PART_DELIVERED)")

                    if hasattr(ev, 'programmedEvent'):
                        programmed_event_time_unix = int(time.mktime(ev.programmedEvent.timestamp.timetuple()))
                    else:
                        programmed_event_time_unix = 0

                    history_dual_bolus_part_delivered = (
                        event_time_utc,
                        event_time_local,
                        ev.bolus_source_name,
                        ev.bolus_source,
                        ev.bolus_number,
                        ev.preset_bolus_number_name,
                        ev.normal_programmed_amount,
                        ev.square_programmed_amount,
                        ev.programmed_duration,
                        ev.active_insulin,
                        ev.delivered_amount,
                        ev.bolus_part_name,
                        ev.bolus_part,
                        ev.delivered_duration,
                        ev.preset_bolus_number,
                        programmed_event_time_unix,
                        ev.canceled
                    )
                    all_history_dual_bolus_part_delivered.append(history_dual_bolus_part_delivered)

            if ev.event_type == cnl24lib.NGPHistoryEvent.EVENT_TYPE.DUAL_BOLUS_PROGRAMMED:
                if last_record_history_dual_bolus_programmed is None or \
                        last_record_history_dual_bolus_programmed < event_time_utc:
                    # logger.info("Insert the record DB (DUAL_BOLUS_PROGRAMMED)")

                    if hasattr(ev, 'bolusWizardEvent'):
                        bolus_wizard_event_time_unix = int(time.mktime(ev.bolusWizardEvent.timestamp.timetuple()))
                    else:
                        bolus_wizard_event_time_unix = 0

                    history_dual_bolus_programmed = (
                        event_time_utc,
                        event_time_local,
                        ev.bolus_source_name,
                        ev.bolus_source,
                        ev.bolus_number,
                        ev.preset_bolus_number_name,
                        ev.normal_programmed_amount,
                        ev.square_programmed_amount,
                        ev.programmed_amount,
                        ev.programmed_duration,
                        ev.active_insulin,
                        ev.preset_bolus_number,
                        bolus_wizard_event_time_unix
                    )
                    all_history_dual_bolus_programmed.append(history_dual_bolus_programmed)

            if ev.event_type == cnl24lib.NGPHistoryEvent.EVENT_TYPE.SQUARE_BOLUS_PROGRAMMED:
                if last_record_history_square_bolus_programmed is None or \
                        last_record_history_square_bolus_programmed < event_time_utc:
                    # logger.info("Insert the record DB (SQUARE_BOLUS_PROGRAMMED)")

                    if hasattr(ev, 'bolusWizardEvent'):
                        bolus_wizard_event_time_unix = int(time.mktime(ev.bolusWizardEvent.timestamp.timetuple()))
                    else:
                        bolus_wizard_event_time_unix = 0

                    history_square_bolus_programmed = (
                        event_time_utc,
                        event_time_local,
                        ev.bolus_source_name,
                        ev.bolus_source,
                        ev.bolus_number,
                        ev.preset_bolus_number_name,
                        ev.programmed_amount,
                        ev.programmed_duration,
                        ev.active_insulin,
                        ev.preset_bolus_number,
                        bolus_wizard_event_time_unix
                    )
                    all_history_square_bolus_programmed.append(history_square_bolus_programmed)

            if ev.event_type == cnl24lib.NGPHistoryEvent.EVENT_TYPE.SQUARE_BOLUS_DELIVERED:
                if last_record_history_square_bolus_delivered is None or \
                        last_record_history_square_bolus_delivered < event_time_utc:
                    # logger.info("Insert the record DB (DUAL_BOLUS_PART_DELIVERED)")

                    if hasattr(ev, 'programmedEvent'):
                        programmed_event_time_unix = int(time.mktime(ev.programmedEvent.timestamp.timetuple()))
                    else:
                        programmed_event_time_unix = 0

                    history_square_bolus_delivered = (
                        event_time_utc,
                        event_time_local,
                        ev.bolus_source_name,
                        ev.bolus_source,
                        ev.bolus_number,
                        ev.preset_bolus_number_name,
                        ev.programmed_amount,
                        ev.active_insulin,
                        ev.delivered_amount,
                        ev.preset_bolus_number,
                        ev.programmed_duration,
                        ev.delivered_duration,
                        programmed_event_time_unix,
                        ev.canceled
                    )
                    all_history_square_bolus_delivered.append(history_square_bolus_delivered)

    if len(all_history_bolus_wizard_estimate) > 0:
        logger.info("Insert(s): {0} to DB(bolus_wizard_estimate)".format(len(all_history_bolus_wizard_estimate)))
        db_connect.insert_history_bolus_wizard_estimate(all_history_bolus_wizard_estimate)

    if len(all_history_dual_bolus_part_delivered) > 0:
        logger.info("Insert(s): {0} to DB(dual_bolus_delivered)".format(len(all_history_dual_bolus_part_delivered)))
        db_connect.insert_history_dual_bolus_part_delivered(all_history_dual_bolus_part_delivered)

    if len(all_history_dual_bolus_programmed) > 0:
        logger.info("Insert(s): {0} to DB(dual_bolus_programmed)".format(len(all_history_dual_bolus_programmed)))
        db_connect.insert_history_dual_bolus_programmed(all_history_dual_bolus_programmed)

    if len(all_history_normal_bolus_programmed) > 0:
        logger.info("Insert(s): {0} to DB(normal_bolus_programmed)".format(len(all_history_normal_bolus_programmed)))
        db_connect.insert_history_normal_bolus_programmed(all_history_normal_bolus_programmed)

    if len(all_history_normal_bolus_delivered) > 0:
        logger.info("Insert(s): {0} to DB(normal_bolus_delivered)".format(len(all_history_normal_bolus_delivered)))
        db_connect.insert_history_normal_bolus_delivered(all_history_normal_bolus_delivered)

    if len(all_history_square_bolus_programmed) > 0:
        logger.info("Insert(s): {0} to DB(square_bolus_programmed)".format(len(all_history_square_bolus_programmed)))
        db_connect.insert_history_square_bolus_programmed(all_history_square_bolus_programmed)

    if len(all_history_square_bolus_delivered) > 0:
        logger.info("Insert(s): {0} to DB(square_bolus_delivered)".format(len(all_history_square_bolus_delivered)))
        db_connect.insert_history_square_bolus_delivered(all_history_square_bolus_delivered)

    if len(all_daily_totals) > 0:
        logger.info("Insert(s): {0} to DB(daily_totals)".format(len(all_daily_totals)))
        db_connect.insert_history_daily_totals(all_daily_totals)


if __name__ == '__main__':

    logging.info("Start")

    local_timezone = pytz.timezone("Europe/Samara")

    # get_pump_data(10)
    parsing_history()

    diary.fill_diary(2)
    sys.exit(0)
