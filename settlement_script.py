import pandas as pd
import numpy as np
import holidays
from datetime import timedelta, datetime, time as dtime
from google.cloud import bigquery
import os

pd.set_option('future.no_silent_downcasting', True)

print("⏳ Memulai Proses Settlement Otomatis...")
PROJECT_ID = 'youtap-indonesia-bi'
client = bigquery.Client(project=PROJECT_ID)

# ==============================================================================
# STEP 1: DAFTAR HARI LIBUR NASIONAL 2026
# ==============================================================================
manual_holidays = [
    datetime(2026, 1, 1).date(),   # Tahun Baru
    datetime(2026, 1, 16).date(),
    datetime(2026, 2, 17).date(),  # Imlek
    datetime(2026, 3, 19).date(),
    datetime(2026, 3, 21).date(),
    datetime(2026, 3, 22).date(),
    datetime(2026, 4, 3).date(),
    datetime(2026, 5, 1).date(),   # Hari Buruh
    datetime(2026, 5, 14).date(),
    datetime(2026, 5, 27).date(),
    datetime(2026, 5, 31).date(),
    datetime(2026, 6, 1).date(),
    datetime(2026, 8, 17).date(),  # HUT RI
    datetime(2026, 12, 25).date(), # Natal
]

# ==============================================================================
# STEP 2: BIGQUERY SQL
# ==============================================================================
QUERY_SETTLE = """
WITH merchants AS (
    SELECT sm.*,
        CASE
            WHEN sm.account_id IN (89236,90181,88552,88116,65440,2553,2568,73707,87227,
                                  4194,82784,82947,86202,86989,85961,86015,86034,86275,
                                  86288,86473,86480,86506,86513,86759,86948,94399,86738,
                                  51873,113441,119398,117910,121980,140668,142342,147566,
                                  148766,150562,154443,157985,302966,302155) THEN 0
            WHEN sm.account_id = 128013 THEN 0.015
            WHEN referral_code_acquisition = '98ENTP062' THEN 0.02
            WHEN referral_code_acquisition = '99ENTP017' THEN 0.012
            WHEN referral_code_acquisition = '99ENTP018' THEN 0.012
            WHEN referral_code_acquisition = '98ENTP029' THEN 0.017
            WHEN referral_code_acquisition = '98ENTP011' THEN 0.006
            WHEN referral_code_acquisition = '98ENTP017' THEN 0.006
            WHEN referral_code_acquisition = '99ENTP003' THEN 0.01
            WHEN referral_code_acquisition = '98ENTP026' THEN 0.015
            WHEN referral_code_acquisition = '98ENTP032' THEN 0.017
            WHEN referral_code_acquisition = '98ENTP014' THEN 0.013
            ELSE 0.007
        END AS mdr_rate
    FROM `youtap-indonesia-bi.summary.merchants` sm
),
DATA_YTI_BASE AS (
    SELECT
        COALESCE(
            b.ref_no, c.approval_code, d.ref_no,
            LPAD(e.reff_id, 12, '0'), f.transaction_id,
            g.refference_number, RIGHT(h.merchantinvoice, 18),
            i.transaction_id, j.transidmerchant, k.reference_no,
            l.retrieval_reference_number,
            IFNULL(m.issuer_rrn, n.reference_number)
        ) AS key_join_issuer,
        txn_date, DATE(txn_date) AS trx_date, a.issuer,
        a.account_id, a.yti_merchant_id AS merchant_id,
        a.yti_merchant_name AS merch_name,
        sm.referral_code_acquisition, sm.vendor_type,
        a.yti_txn_extref, a.yti_txn_ref, a.issuer_txn_ref,
        mcd.approval_code AS mcd_approval_code,
        a.credit_trans_trx_id, a.terminal_id,
        a.txn_type, a.amt,
        (CASE WHEN a.txn_type = 'Customer Purchase' THEN a.amt ELSE 0 END)
        - (CASE WHEN a.txn_type = 'Emoney Reversal' THEN a.amt ELSE 0 END) AS amount_base,
        sm.mdr_rate
    FROM `youtap-indonesia-bi.datawarehouses.yti_settlement_report_hourly` a
    LEFT JOIN merchants sm ON a.account_id = sm.account_id
    LEFT JOIN (
        SELECT trx_id FROM `youtap-indonesia-bi.summary.transactions`
        WHERE DATE(trx_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 65 DAY)
    ) st ON CAST(a.credit_trans_trx_id AS STRING) = st.trx_id
    LEFT JOIN (
        SELECT yt_ref, approval_code
        FROM `youtap-indonesia-bi.datawarehouses.yti_settlement_mcd_report_hourly`
        WHERE DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 65 DAY)
    ) mcd ON st.trx_id = CAST(mcd.yt_ref AS STRING)
    LEFT JOIN (SELECT * FROM `youtap-indonesia-bi.datawarehouses.recon_linkaja_msme_report`
        WHERE DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
    ) b ON b.ref_no = a.issuer_txn_ref AND DATE(a.txn_date) = DATE(b.transaction_date)
    LEFT JOIN (
        SELECT transaction_date,
            REGEXP_REPLACE(sequence_id, r'_payment', '') AS approval_code,
            SUM(transaction_amount) AS transaction_amount,
            SUM(mdr) AS mdr, SUM(net_amount) AS net_amount
        FROM `youtap-indonesia-bi.datawarehouses.recon_linkaja_mcd_report`
        WHERE DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
        GROUP BY 1,2
    ) c ON c.approval_code = mcd.approval_code AND DATE(a.txn_date) = DATE(c.transaction_date)
    LEFT JOIN (SELECT * FROM `youtap-indonesia-bi.datawarehouses.recon_linkaja_expansion_report`
        WHERE DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
    ) d ON d.ref_no = a.issuer_txn_ref AND DATE(a.txn_date) = DATE(d.transaction_date)
    LEFT JOIN (SELECT * FROM `youtap-indonesia-bi.datawarehouses.recon_bni_bank_report`
        WHERE DATE(trx_datetime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
    ) e ON LPAD(e.reff_id, 12, '0') = a.yti_txn_extref AND DATE(a.txn_date) = DATE(e.trx_datetime)
    LEFT JOIN (SELECT * FROM `youtap-indonesia-bi.datawarehouses.recon_shopeepay_report`
        WHERE DATE(create_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
    ) f ON f.transaction_id = a.yti_txn_extref AND DATE(a.txn_date) = DATE(f.create_time)
    LEFT JOIN (SELECT * FROM `youtap-indonesia-bi.datawarehouses.recon_mandiri_bank_report`
        WHERE DATE(trxdate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
    ) g ON g.refference_number = a.yti_txn_extref AND DATE(a.txn_date) = DATE(g.trxdate)
    LEFT JOIN (SELECT * FROM `youtap-indonesia-bi.datawarehouses.recon_ovo_report`
        WHERE DATE(transactiondate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
    ) h ON RIGHT(h.merchantinvoice, 18) = a.issuer_txn_ref AND DATE(a.txn_date) = DATE(h.transactiondate)
    LEFT JOIN (SELECT * FROM `youtap-indonesia-bi.datawarehouses.recon_kredivo_report`
        WHERE DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
    ) i ON i.transaction_id = a.yti_txn_extref AND DATE(a.txn_date) = DATE(i.transaction_date)
    LEFT JOIN (SELECT * FROM `youtap-indonesia-bi.datawarehouses.recon_indodana_report`
        WHERE DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
    ) j ON j.transidmerchant = a.yti_txn_extref AND DATE(a.txn_date) = DATE(j.transaction_date)
    LEFT JOIN (SELECT * FROM `youtap-indonesia-bi.datawarehouses.recon_issuer_bca_bank_file_report`
        WHERE DATE(payment_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
    ) k ON k.reference_no = a.yti_txn_extref AND DATE(a.txn_date) = DATE(k.payment_date)
    LEFT JOIN (SELECT * FROM `youtap-indonesia-bi.datawarehouses.recon_issuer_btn_bank_file_report`
        WHERE DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
    ) l ON LPAD(CAST(l.retrieval_reference_number AS STRING), 12, '0') = a.yti_txn_extref
        AND DATE(a.txn_date) = DATE(l.transaction_date)
    LEFT JOIN (SELECT * FROM `youtap-indonesia-bi.datawarehouses.recon_issuer_ottopay_bank_file_report`
        WHERE DATE(transaction_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
    ) m ON m.issuer_rrn = a.yti_txn_extref AND DATE(a.txn_date) = DATE(m.transaction_time)
    LEFT JOIN (SELECT * FROM `youtap-indonesia-bi.datawarehouses.recon_issuer_ottopay_dashboard_bank_file_report`
        WHERE DATE(transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
    ) n ON n.reference_number = a.yti_txn_extref AND DATE(a.txn_date) = DATE(n.transaction_date)
    WHERE DATE(a.txn_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
      AND sm.vendor_id != 1602
      AND sm.account_id NOT IN (367540, 367620, 287527)
)
SELECT * FROM DATA_YTI_BASE 
"""

QUERY_VOUCHER = """
SELECT
    a.txn_date, DATE(a.txn_date) AS trx_date, a.account_id, a.yti_merchant_id AS merchant_id,
    a.issuer,
    (CASE WHEN a.txn_type = 'Customer Purchase' THEN a.amt ELSE 0 END)
    - (CASE WHEN a.txn_type = 'Emoney Reversal' THEN a.amt ELSE 0 END) AS amount_base
FROM `youtap-indonesia-bi.datawarehouses.yti_settlement_report_hourly` a
LEFT JOIN `youtap-indonesia-bi.summary.merchants` sm ON a.account_id = sm.account_id
WHERE DATE(a.txn_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
  AND a.issuer IN ('MCD_VOUCHER','YTI_VOUCHER','SNAP PROD Settlement')
  AND sm.vendor_id != 1602
  AND sm.account_id NOT IN (367540, 367620, 287527)
"""

QUERY_BANK = "SELECT * FROM `youtap-indonesia-bi.views.merchant_bank_accounts_v`"

QUERY_OPS = """
SELECT * FROM `youtap-indonesia-bi.datawarehouses.ops_balance_report`
WHERE DATE(processed_dttm) >= DATE_SUB(CURRENT_DATE(), INTERVAL 75 DAY)
"""

QUERY_FIRST_TRX = """
WITH merchants AS (
    SELECT sm.*,
        CASE
            WHEN sm.account_id IN (89236,90181,88552,88116,65440,2553,2568,73707,87227,
                                  4194,82784,82947,86202,86989,85961,86015,86034,86275,
                                  86288,86473,86480,86506,86513,86759,86948,94399,86738,
                                  51873,113441,119398,117910,121980,140668,142342,147566,
                                  148766,150562,154443,157985,302966,302155) THEN 0
            WHEN sm.account_id = 128013 THEN 0.015
            WHEN referral_code_acquisition = '98ENTP062' THEN 0.02
            WHEN referral_code_acquisition = '99ENTP017' THEN 0.012
            WHEN referral_code_acquisition = '99ENTP018' THEN 0.012
            WHEN referral_code_acquisition = '98ENTP029' THEN 0.017
            WHEN referral_code_acquisition = '98ENTP011' THEN 0.006
            WHEN referral_code_acquisition = '98ENTP017' THEN 0.006
            WHEN referral_code_acquisition = '99ENTP003' THEN 0.01
            WHEN referral_code_acquisition = '98ENTP026' THEN 0.015
            WHEN referral_code_acquisition = '98ENTP032' THEN 0.017
            WHEN referral_code_acquisition = '98ENTP014' THEN 0.013
            ELSE 0.007
        END AS mdr_rate
    FROM `youtap-indonesia-bi.summary.merchants` sm
)
SELECT
    txn_date,
    a.account_id,
    SUM(
        (CASE WHEN a.txn_type = 'Customer Purchase' THEN a.amt ELSE 0 END)
        - (CASE WHEN a.txn_type = 'Emoney Reversal' THEN a.amt ELSE 0 END)
    ) AS amount,
    ROUND(SUM(
        ((CASE WHEN a.txn_type = 'Customer Purchase' THEN a.amt ELSE 0 END)
        - (CASE WHEN a.txn_type = 'Emoney Reversal' THEN a.amt ELSE 0 END)) * sm.mdr_rate
    )) AS mdr_amount,
    ROUND(SUM(
        (CASE WHEN a.txn_type = 'Customer Purchase' THEN a.amt ELSE 0 END)
        - (CASE WHEN a.txn_type = 'Emoney Reversal' THEN a.amt ELSE 0 END)
    ) - SUM(
        ((CASE WHEN a.txn_type = 'Customer Purchase' THEN a.amt ELSE 0 END)
        - (CASE WHEN a.txn_type = 'Emoney Reversal' THEN a.amt ELSE 0 END)) * sm.mdr_rate
    )) AS nett_amount
FROM `youtap-indonesia-bi.datawarehouses.yti_settlement_report_hourly` a
LEFT JOIN merchants sm ON a.account_id = sm.account_id
WHERE DATE(a.txn_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
  AND TIME(a.txn_date) BETWEEN '00:00:00' AND '00:01:00'
  AND sm.vendor_id != 1602
  AND sm.account_id NOT IN (367540, 367620, 287527)
GROUP BY 1, 2
QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE(txn_date), account_id ORDER BY txn_date ASC) = 1
"""

QUERY_TRX_00 = """
WITH merchants AS (
    SELECT sm.*
    FROM `youtap-indonesia-bi.summary.merchants` sm
)
SELECT
    DATE(txn_date) AS trx_date,
    a.account_id,
    SUM(
        (CASE WHEN a.txn_type = 'Customer Purchase' THEN a.amt ELSE 0 END)
        - (CASE WHEN a.txn_type = 'Emoney Reversal' THEN a.amt ELSE 0 END)
    ) AS amount
FROM `youtap-indonesia-bi.datawarehouses.yti_settlement_report_hourly` a
LEFT JOIN merchants sm ON a.account_id = sm.account_id
WHERE DATE(a.txn_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND TIME(a.txn_date) BETWEEN '00:00:00' AND '00:01:01'
  AND EXTRACT(DAYOFWEEK FROM DATE(a.txn_date)) BETWEEN 2 AND 6
  AND sm.vendor_id != 1602
  AND sm.account_id NOT IN (367540, 367620, 287527)
GROUP BY 1, 2
"""

# ==============================================================================
# STEPS 3-6: SETTLEMENT ENGINE V15.6
# ==============================================================================
class YoutapSettlementEngine:

    EXCLUDED_REFS = {'99ENTP001','99ENTP004','99ENTP014','99ENTP019','99ENTP023','99ENTP026'}

    def __init__(self, year=2026):
        self.id_hols = holidays.Indonesia(years=[year, year-1])
        for d in manual_holidays:
            self.id_hols[d] = "Libur Nasional"

    def _is_settlement_day(self, settle_date):
        if settle_date.weekday() >= 5:  # Sabtu, Minggu
            return False
        if settle_date in self.id_hols:
            return False
        return True

    def _get_last_working_day(self, current_date):
        prev_date = current_date - timedelta(days=1)
        while not self._is_settlement_day(prev_date):
            prev_date -= timedelta(days=1)
        return prev_date

    def run_process(self, df_base, df_voucher, df_bank, df_ops, df_first_trx, df_trx00_bq):

        for df in [df_base, df_voucher, df_ops, df_bank, df_first_trx, df_trx00_bq]:
            df.columns = [c.upper() for c in df.columns]

        df_base = df_base.drop_duplicates(subset=['CREDIT_TRANS_TRX_ID']).copy()
        df_bank = df_bank.sort_values(['ACCOUNT_ID','BANK_NAME']).drop_duplicates(
            subset=['ACCOUNT_ID'], keep='first'
        )

        df_base = df_base[
            (~df_base['REFERRAL_CODE_ACQUISITION'].isin(self.EXCLUDED_REFS)) |
            (df_base['REFERRAL_CODE_ACQUISITION'].isna())
        ].copy()

        df_unmatched = df_base[df_base['KEY_JOIN_ISSUER'].isna()].copy()
        
        df_unmatched['AMOUNT_BASE'] = pd.to_numeric(df_unmatched['AMOUNT_BASE'], errors='coerce').fillna(0.0)
        df_unmatched['MDR_RATE'] = pd.to_numeric(df_unmatched['MDR_RATE'], errors='coerce').fillna(0.007)
        df_unmatched['TXN_DATE'] = pd.to_datetime(df_unmatched['TXN_DATE'])
        df_unmatched['BALANCE_DATE'] = df_unmatched['TXN_DATE'].dt.date
        df_unmatched['MDR_AMT'] = df_unmatched['AMOUNT_BASE'] * df_unmatched['MDR_RATE']
        df_unmatched['NET_AMT'] = df_unmatched['AMOUNT_BASE'] - df_unmatched['MDR_AMT']
        
        today_date = datetime.now().date()
        df_unmatched = df_unmatched[df_unmatched['BALANCE_DATE'] < today_date]
        
        not_match_agg = df_unmatched.groupby(['ACCOUNT_ID', 'BALANCE_DATE']).agg(
            TOTAL_TRAFFIC_NOT_MATCH=('CREDIT_TRANS_TRX_ID', 'nunique'),
            TOTAL_GROSS_TRANSACTION_NOT_MATCH=('AMOUNT_BASE', 'sum'),
            TOTAL_MDR_NOT_MATCH=('MDR_AMT', 'sum'),
            TOTAL_AMOUNT_TO_TRANSFER_NOT_MATCH=('NET_AMT', 'sum')
        ).reset_index()
        not_match_agg['TOTAL_MDR_NOT_MATCH'] = not_match_agg['TOTAL_MDR_NOT_MATCH'].round()
        not_match_agg['TOTAL_AMOUNT_TO_TRANSFER_NOT_MATCH'] = not_match_agg['TOTAL_AMOUNT_TO_TRANSFER_NOT_MATCH'].round()
        
        df_base = df_base[df_base['KEY_JOIN_ISSUER'].notna()].copy()

        df_base['AMOUNT_BASE'] = pd.to_numeric(df_base['AMOUNT_BASE'], errors='coerce').fillna(0.0)
        df_base['MDR_RATE'] = pd.to_numeric(df_base['MDR_RATE'], errors='coerce').fillna(0.007)
        df_base['TXN_DATE'] = pd.to_datetime(df_base['TXN_DATE'])

        df_base['TRX_DATE'] = df_base['TXN_DATE'].dt.date
        df_base['BALANCE_DATE'] = df_base['TRX_DATE']

        df_voucher['AMOUNT_BASE'] = pd.to_numeric(df_voucher['AMOUNT_BASE'], errors='coerce').fillna(0.0)
        df_voucher['TXN_DATE'] = pd.to_datetime(df_voucher['TXN_DATE'])
        df_voucher['VOUCH_DATE'] = (df_voucher['TXN_DATE'] + timedelta(days=1)).dt.date
        vouch_agg = df_voucher.groupby(['ACCOUNT_ID','VOUCH_DATE'])['AMOUNT_BASE'].sum().reset_index()
        vouch_agg = vouch_agg.rename(columns={'AMOUNT_BASE': 'AMOUNT_VOUCHER', 'VOUCH_DATE': 'BALANCE_DATE'})

        daily = df_base.groupby(['ACCOUNT_ID','BALANCE_DATE']).agg(
            AMOUNT_BASE=('AMOUNT_BASE','sum'),
            MDR_RATE=('MDR_RATE','max'),
            MERCH_NAME=('MERCH_NAME','first')
        ).reset_index()

        daily = daily.merge(vouch_agg, on=['ACCOUNT_ID','BALANCE_DATE'], how='left')
        daily['AMOUNT_VOUCHER'] = daily['AMOUNT_VOUCHER'].fillna(0.0)

        df_ops['BALANCE'] = pd.to_numeric(df_ops['BALANCE'], errors='coerce').fillna(0.0)
        df_ops['OPS_DATE'] = pd.to_datetime(df_ops['CREATED_DATE']).dt.date
        df_ops_clean = df_ops.sort_values(
            ['ACCOUNT_ID','OPS_DATE','PROCESSED_DTTM']
        ).drop_duplicates(subset=['ACCOUNT_ID','OPS_DATE'], keep='first').copy()

        df_base['TXN_TIME_WIB'] = df_base['TXN_DATE'].dt.time
        df_base['TRX_DATE_ONLY'] = df_base['TXN_DATE'].dt.date
        df_base['TRX_DAYOFWEEK_WIB'] = df_base['TXN_DATE'].dt.dayofweek

        df_trx00_bq['TRX_DATE'] = pd.to_datetime(df_trx00_bq['TRX_DATE'])
        df_trx00_bq['AMOUNT'] = pd.to_numeric(df_trx00_bq['AMOUNT'], errors='coerce').fillna(0.0)
        df_trx00 = df_trx00_bq[['ACCOUNT_ID','TRX_DATE','AMOUNT']].copy()
        df_trx00['TRX_DATE_ONLY'] = df_trx00['TRX_DATE'].dt.date
        df_trx00 = df_trx00.groupby(['ACCOUNT_ID','TRX_DATE_ONLY'])['AMOUNT'].sum().reset_index()
        df_trx00 = df_trx00.rename(columns={'AMOUNT': 'TRX_00_AMT'})

        df_first_trx['TXN_DATE'] = pd.to_datetime(df_first_trx['TXN_DATE'])
        df_first_trx['AMOUNT'] = pd.to_numeric(df_first_trx['AMOUNT'], errors='coerce').fillna(0.0)
        df_first_trx['FT_TRX_DATE'] = df_first_trx['TXN_DATE'].dt.date
        df_ft = df_first_trx[['ACCOUNT_ID','FT_TRX_DATE','AMOUNT']].copy()
        df_ft = df_ft.rename(columns={'AMOUNT': 'FIRST_TRX'})

        df_trx_holiday = df_base[
            (df_base['TXN_TIME_WIB'] <= dtime(0, 1, 0))
        ].copy()
        
        holiday_carry_dates = []
        for d in df_trx_holiday['TRX_DATE_ONLY'].unique():
            if self._is_settlement_day(d) and not self._is_settlement_day(d + timedelta(days=1)):
                holiday_carry_dates.append(d)
                
        df_trx_holiday = df_trx_holiday[df_trx_holiday['TRX_DATE_ONLY'].isin(holiday_carry_dates)]
        df_trx_holiday = df_trx_holiday.groupby(['ACCOUNT_ID','TRX_DATE_ONLY'])['AMOUNT_BASE'].sum().reset_index()
        df_trx_holiday = df_trx_holiday.rename(columns={'AMOUNT_BASE': 'TRX_HOLIDAY_CARRY_AMT'})

        min_dt, max_dt = daily['BALANCE_DATE'].min(), daily['BALANCE_DATE'].max()
        max_dt_extended = max_dt + timedelta(days=7)
        all_dates = pd.date_range(start=min_dt, end=max_dt_extended).date
        all_accs = daily[['ACCOUNT_ID','MERCH_NAME']].drop_duplicates(subset=['ACCOUNT_ID'], keep='last')
        spine = all_accs.assign(key=1).merge(
            pd.DataFrame({'BALANCE_DATE': all_dates, 'key': 1}), on='key'
        ).drop('key', axis=1)

        final = spine.merge(daily, on=['ACCOUNT_ID','BALANCE_DATE'], how='left',
                            suffixes=('','_daily'))
        if 'MERCH_NAME_daily' in final.columns:
            final['MERCH_NAME'] = final['MERCH_NAME'].fillna(final['MERCH_NAME_daily'])
            final.drop('MERCH_NAME_daily', axis=1, inplace=True)

        final['BALANCE_DATE_PLUS1'] = final['BALANCE_DATE'].apply(lambda d: d + timedelta(days=1))

        final = final.merge(
            df_ops_clean[['ACCOUNT_ID','OPS_DATE','BALANCE']],
            left_on=['ACCOUNT_ID','BALANCE_DATE_PLUS1'],
            right_on=['ACCOUNT_ID','OPS_DATE'], how='left'
        )

        final['BALANCE_RAW'] = final['BALANCE']

        final = final.sort_values(['ACCOUNT_ID','BALANCE_DATE'])
        final['BALANCE'] = final['BALANCE'].fillna(0.0)
        final['MDR_RATE'] = final.groupby('ACCOUNT_ID')['MDR_RATE'].ffill().bfill().fillna(0.007)

        df_trx00['TRX_DATE_PLUS1'] = df_trx00['TRX_DATE_ONLY'].apply(lambda d: d + timedelta(days=1))
        final = final.merge(df_trx00,
                            left_on=['ACCOUNT_ID','BALANCE_DATE_PLUS1'],
                            right_on=['ACCOUNT_ID','TRX_DATE_PLUS1'], how='left')
        
        final['TRX_00_AMT'] = pd.to_numeric(final['TRX_00_AMT'], errors='coerce').fillna(0.0)
        final['BALANCE'] = final['BALANCE'] + final['TRX_00_AMT']

        final['LAST_WORKING_DAY'] = final['BALANCE_DATE_PLUS1'].apply(lambda d: self._get_last_working_day(d))

        final = final.merge(df_trx_holiday,
                            left_on=['ACCOUNT_ID','LAST_WORKING_DAY'],
                            right_on=['ACCOUNT_ID','TRX_DATE_ONLY'], how='left',
                            suffixes=('','_hol'))

        final = final.merge(
            df_ft[['ACCOUNT_ID','FT_TRX_DATE','FIRST_TRX']],
            left_on=['ACCOUNT_ID','BALANCE_DATE_PLUS1'],
            right_on=['ACCOUNT_ID','FT_TRX_DATE'], how='left'
        )
        
        for c in ['FIRST_TRX','AMOUNT_VOUCHER','AMOUNT_BASE','TRX_HOLIDAY_CARRY_AMT']:
            final[c] = pd.to_numeric(final[c], errors='coerce').fillna(0.0)
            
        final.drop(columns=['FT_TRX_DATE', 'TRX_DATE_ONLY'], inplace=True, errors='ignore')

        final['BALANCE_ADJ'] = final['BALANCE'] + final['TRX_HOLIDAY_CARRY_AMT']

        final['TOTAL_GROSS_BALANCE'] = np.where(
            np.isclose(final['BALANCE_ADJ'] - final['AMOUNT_BASE'],
                       final['FIRST_TRX'], atol=1.0),
            final['BALANCE_ADJ'] - final['FIRST_TRX'],
            final['BALANCE_ADJ']
        )
        final['BALANCE_FINAL'] = final['TOTAL_GROSS_BALANCE'] - final['AMOUNT_VOUCHER']
        final['GROSS_DIFFERENCE'] = final['BALANCE_ADJ'] - final['AMOUNT_BASE']

        processed = []
        for acc_id, group in final.groupby('ACCOUNT_ID'):
            ember_acc = 0.0
            ember_vouch = 0.0
            ember_ft = 0.0
            first_b_date_in_batch = None

            for _, row in group.iterrows():
                b_date = row['BALANCE_DATE']
                settle_date = b_date + timedelta(days=1)
                is_biz = self._is_settlement_day(settle_date)

                if row['AMOUNT_BASE'] != 0 and first_b_date_in_batch is None:
                    first_b_date_in_batch = b_date

                ember_acc += row['AMOUNT_BASE']
                ember_vouch += row['AMOUNT_VOUCHER']
                ember_ft += row['FIRST_TRX']

                if is_biz and ember_acc != 0:
                    diff_emit = (row['BALANCE_ADJ'] - ember_vouch) - ember_acc

                    if not self._is_settlement_day(settle_date - timedelta(days=1)):
                        last_work_date = self._get_last_working_day(settle_date)
                        match = group[group['BALANCE_DATE_PLUS1'] == last_work_date]

                        if not match.empty:
                            raw_bal = match.iloc[0]['BALANCE_RAW']
                            if pd.isna(raw_bal):
                                emit_carry = 0.0
                            else:
                                bal_val = float(raw_bal)
                                emit_carry = bal_val if bal_val < 20140.0 else 0.0
                        else:
                            emit_carry = 0.0
                    else:
                        if first_b_date_in_batch is None:
                            first_b_date_in_batch = b_date

                        prev_date = first_b_date_in_batch - timedelta(days=1)
                        match = group[group['BALANCE_DATE'] == prev_date]
                        if not match.empty:
                            prev_bal = match.iloc[0]['BALANCE']
                            emit_carry = float(prev_bal) if float(prev_bal) < 20140.0 else 0.0
                        else:
                            emit_carry = 0.0

                    amount_final = ember_acc - ember_vouch + emit_carry
                    total_mdr = amount_final * row['MDR_RATE']
                    net_est = amount_final - total_mdr

                    if net_est < 20140:
                        confirm = 'MINIMUM NOT MET'
                    elif abs(amount_final - row['BALANCE_FINAL']) > 1:
                        confirm = 'NOT MATCH'
                    else:
                        confirm = 'CONFIRM'

                    res = row.copy().to_dict()
                    res['BALANCE_DATE'] = settle_date
                    res.update({
                        'CARRY_OVER_BALANCE': emit_carry,
                        'FIRST_TRX': ember_ft,
                        'TOTAL_GROSS_TRANSACTION': ember_acc,
                        'GROSS_DIFFERENCE': ember_acc - row['TOTAL_GROSS_BALANCE'],
                        'AMOUNT_VOUCHER': ember_vouch,
                        'AMOUNT_FINAL': amount_final,
                        'TOTAL_MDR': total_mdr,
                        'TOTAL_NET_TRX': net_est,
                        'AMOUNT_TO_TRANSFER': net_est,
                        'CONFIRM_TO_TRANSFER': confirm,
                    })
                    processed.append(res)

                    ember_acc = 0.0
                    ember_vouch = 0.0
                    ember_ft = 0.0
                    first_b_date_in_batch = None

        if not processed:
            return pd.DataFrame()

        result_df = pd.DataFrame(processed).merge(df_bank, on='ACCOUNT_ID', how='left')
        result_df = result_df.merge(not_match_agg, left_on=['ACCOUNT_ID', 'BALANCE_DATE'], right_on=['ACCOUNT_ID', 'BALANCE_DATE'], how='left')
        
        for c in ['TOTAL_TRAFFIC_NOT_MATCH', 'TOTAL_GROSS_TRANSACTION_NOT_MATCH', 'TOTAL_MDR_NOT_MATCH', 'TOTAL_AMOUNT_TO_TRANSFER_NOT_MATCH']:
            result_df[c] = pd.to_numeric(result_df[c], errors='coerce').fillna(0.0)

        result_df['TOTAL_GROSS_BALANCE'] = np.where(
            np.isclose(
                result_df['BALANCE_ADJ'] - result_df['TOTAL_GROSS_TRANSACTION'],
                result_df['FIRST_TRX'], atol=1.0
            ) & (result_df['FIRST_TRX'] > 0),
            result_df['BALANCE_ADJ'] - result_df['FIRST_TRX'],
            result_df['BALANCE_ADJ']
        )
        result_df['GROSS_DIFFERENCE'] = result_df['TOTAL_GROSS_TRANSACTION'] - result_df['TOTAL_GROSS_BALANCE']
        result_df['BALANCE_FINAL'] = result_df['TOTAL_GROSS_BALANCE'] - result_df['AMOUNT_VOUCHER']

        result_df['CONFIRM_TO_TRANSFER'] = np.where(
            result_df['TOTAL_NET_TRX'] < 20140, 'MINIMUM NOT MET',
            np.where(
                abs(result_df['AMOUNT_FINAL'] - result_df['BALANCE_FINAL']) > 1,
                'NOT MATCH', 'CONFIRM'
            )
        )

        result_df = result_df.sort_values(by=['BALANCE_DATE','ACCOUNT_ID'], ascending=[False, True])

        # ==============================================================================
        # FILTER 14 HARI TERAKHIR DI TERAPKAN DI SINI
        # ==============================================================================
        start_date = today_date - timedelta(days=14)
        result_df = result_df[(result_df['BALANCE_DATE'] >= start_date) & (result_df['BALANCE_DATE'] <= today_date)]

        cols_final = [
            'BALANCE_DATE','ACCOUNT_ID','MERCH_NAME','CARRY_OVER_BALANCE',
            'TOTAL_GROSS_TRANSACTION','TOTAL_GROSS_BALANCE','GROSS_DIFFERENCE',
            'FIRST_TRX','TRX_00_AMT','AMOUNT_VOUCHER','AMOUNT_FINAL','BALANCE_FINAL',
            'TOTAL_MDR','TOTAL_NET_TRX','AMOUNT_TO_TRANSFER',
            'CONFIRM_TO_TRANSFER','BANK_ACCOUNT_NAME','BANK_ACCOUNT_NUMBER',
            'BANK_CODE','BANK_NAME',
            'TOTAL_TRAFFIC_NOT_MATCH','TOTAL_GROSS_TRANSACTION_NOT_MATCH',
            'TOTAL_MDR_NOT_MATCH','TOTAL_AMOUNT_TO_TRANSFER_NOT_MATCH'
        ]
        return result_df[[c for c in cols_final if c in result_df.columns]]

# ==============================================================================
# EXECUTION
# ==============================================================================
print("⏳ Menarik Data dari BigQuery...")
df_b = client.query(QUERY_SETTLE).to_dataframe()
df_v = client.query(QUERY_VOUCHER).to_dataframe()
df_k = client.query(QUERY_BANK).to_dataframe()
df_o = client.query(QUERY_OPS).to_dataframe()
df_ft_bq = client.query(QUERY_FIRST_TRX).to_dataframe()
df_trx00_bq = client.query(QUERY_TRX_00).to_dataframe()

print("\n⏳ Memproses Settlement V15.6...")
engine = YoutapSettlementEngine(year=2026)
df_final = engine.run_process(df_b, df_v, df_k, df_o, df_ft_bq, df_trx00_bq)

if not df_final.empty:
    print(f"✅ Data diproses! Total baris: {len(df_final)}")
    confirm_count = (df_final['CONFIRM_TO_TRANSFER'] == 'CONFIRM').sum()
    not_match_count = (df_final['CONFIRM_TO_TRANSFER'] == 'NOT MATCH').sum()
    min_count = (df_final['CONFIRM_TO_TRANSFER'] == 'MINIMUM NOT MET').sum()
    print(f"   CONFIRM: {confirm_count} | NOT MATCH: {not_match_count} | MINIMUM NOT MET: {min_count}")
    
    # ==========================================================================
    # UPLOAD & REPLACE DATA KE BIGQUERY (14 Hari Terakhir)
    # ==========================================================================
    print("\n⏳ Mengunggah data ke BigQuery...")
    table_id = f"{PROJECT_ID}.summary.settlement_report"
    
    today_date = datetime.now().date()
    start_date = today_date - timedelta(days=14)
    
    # 1. Hapus data lama agar tidak duplikat
    delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE DATE(BALANCE_DATE) >= '{start_date}' AND DATE(BALANCE_DATE) <= '{today_date}'
    """
    try:
        client.query(delete_query).result()
        print("   ✓ Pembersihan data lama selesai.")
    except Exception as e:
        print(f"   ⚠️ Tabel mungkin baru dibuat atau error ringan: {e}")

    # 2. Upload Data Baru
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    try:
        job = client.load_table_from_dataframe(df_final, table_id, job_config=job_config)
        job.result()
        print(f"✅ BERHASIL! {job.output_rows} baris telah tersimpan di BigQuery ({table_id})")
    except Exception as e:
        print(f"❌ GAGAL mengunggah ke BigQuery. Error: {e}")
else:
    print("⚠️ Data kosong, tidak ada yang diunggah.")
