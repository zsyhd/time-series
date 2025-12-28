"""
اسکریپت پیش‌پردازش داده‌های Well Time Series
با فرض فاصله دقیقه‌ای بین رکوردها

این اسکریپت:
- تاریخ‌ها را با فرض فاصله 1 دقیقه‌ای اصلاح می‌کند
- مقادیر NULL را پر می‌کند
- ستون‌های خالی را حذف می‌کند
- نام‌ها را استاندارد می‌کند
- well_id اضافه می‌کند
"""

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re

def fix_timestamp_minute_based(ts, index):
    """
    تبدیل تاریخ با فرض فاصله 1 دقیقه‌ای بین رکوردها
    اولین رکورد: 2024-01-01 00:01:00
    دومین رکورد: 2024-01-01 00:02:00
    ...
    """
    if pd.isna(ts) or not isinstance(ts, str):
        base = datetime(2024, 1, 1, 0, 1, 0)
        return base + timedelta(minutes=index)

    ts = ts.strip()
    base = datetime(2024, 1, 1, 0, 1, 0)

    # اگر day داشت (مثل: day1 12:01:00 AM)
    if 'day' in ts.lower():
        try:
            day_match = re.search(r'day\s*[-_]?\s*(\d+)', ts, re.IGNORECASE)
            if day_match:
                day_num = int(day_match.group(1))
                base = datetime(2024, 1, 1) + timedelta(days=day_num - 1)

                time_part = ts[day_match.end():].strip()
                if time_part:
                    for fmt in ["%I:%M:%S %p", "%H:%M:%S", "%H:%M"]:
                        try:
                            t = datetime.strptime(time_part, fmt).time()
                            return datetime.combine(base.date(), t)
                        except:
                            continue

                return base + timedelta(minutes=index)
        except:
            pass

    # فقط زمان (مثل: 00:02:00)
    if ':' in ts and len(ts) < 12:
        try:
            parts = ts.split(':')
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = int(parts[2]) if len(parts) > 2 else 0

            total_minutes = hours * 60 + minutes
            base_time = datetime(2024, 1, 1, 0, 1, 0)
            return base_time + timedelta(minutes=total_minutes - 1)
        except:
            pass

    # fallback: دقیقه‌ای از اول
    return datetime(2024, 1, 1, 0, 1, 0) + timedelta(minutes=index)

def preprocess_data(input_file='MData.json', output_file='MData_Cleaned.json'):
    """پیش‌پردازش کامل داده"""

    print("=" * 80)
    print("🔧 پیش‌پردازش داده‌های Well Time Series")
    print("=" * 80)

    # 1. بارگذاری
    print("\n📥 بارگذاری داده...")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    print(f"   ✓ {len(df)} رکورد × {len(df.columns)} ستون")

    # 2. اصلاح تاریخ‌ها
    print("\n📅 اصلاح تاریخ‌ها (فاصله دقیقه‌ای)...")
    df['original_timestamp'] = df['Timesteap']
    df['timestamp'] = [fix_timestamp_minute_based(ts, i) 
                       for i, ts in enumerate(df['Timesteap'])]
    print(f"   ✓ اولین زمان: {df['timestamp'].iloc[0]}")
    print(f"   ✓ آخرین زمان: {df['timestamp'].iloc[-1]}")
    print(f"   ✓ مدت زمان: {(df['timestamp'].iloc[-1] - df['timestamp'].iloc[0]).days + 1} روز")

    # 3. حذف ستون‌های کاملاً خالی
    print("\n🗑️  حذف ستون‌های کاملاً خالی...")
    empty_cols = []
    for col in df.columns:
        if col not in ['Timesteap', 'timestamp', 'original_timestamp']:
            if df[col].isna().all():
                empty_cols.append(col)
                df = df.drop(col, axis=1)

    if empty_cols:
        print(f"   ✓ حذف شد: {', '.join(empty_cols)}")
    else:
        print("   ✓ ستون خالی یافت نشد")

    # 4. پر کردن NULL ها
    print("\n🔧 پر کردن مقادیر NULL...")
    numeric_cols = ['P-PDG', 'P-TPT', 'T-TPT', 'P-MON-CKP', 
                    'T-JUS-CKP', 'P-JUS-CKGL', 'QGL']

    for col in numeric_cols:
        if col not in df.columns:
            continue

        null_count = df[col].isna().sum()
        if null_count == 0:
            continue

        null_pct = null_count / len(df)

        if null_pct < 0.05:
            df[col] = df[col].interpolate(method='linear', limit_direction='both')
            method = "interpolation"
        elif null_pct < 0.20:
            df[col] = df[col].ffill().bfill()
            method = "forward/backward fill"
        else:
            for cls in df['class'].unique():
                mask = df['class'] == cls
                mean_val = df.loc[mask, col].mean()
                if not pd.isna(mean_val):
                    df.loc[mask & df[col].isna(), col] = mean_val
            df[col] = df[col].fillna(df[col].mean())
            method = "class-based mean"

        filled = null_count - df[col].isna().sum()
        print(f"   ✓ {col}: {filled} NULL ({method})")

    # 5. استاندارد سازی نام‌ها
    print("\n📝 استاندارد سازی نام ستون‌ها...")
    df = df.rename(columns={
        'Timesteap': '_old_timestamp',
        'P-PDG': 'p_pdg',
        'P-TPT': 'p_tpt',
        'T-TPT': 't_tpt',
        'P-MON-CKP': 'p_mon_ckp',
        'T-JUS-CKP': 't_jus_ckp',
        'P-JUS-CKGL': 'p_jus_ckgl',
        'QGL': 'qgl'
    })
    print("   ✓ تبدیل به snake_case")

    # 6. افزودن well_id
    print("\n➕ افزودن metadata...")
    df['well_id'] = 1
    print("   ✓ well_id=1")

    # 7. مرتب‌سازی ستون‌ها
    priority_cols = ['timestamp', 'well_id', 'class']
    sensor_cols = [c for c in df.columns 
                   if c not in priority_cols + ['original_timestamp', '_old_timestamp']]
    sensor_cols.sort()

    final_cols = priority_cols + sensor_cols + ['original_timestamp']
    final_cols = [c for c in final_cols if c in df.columns]
    df = df[final_cols]

    # حذف تکراری‌ها
    df = df.loc[:, ~df.columns.duplicated()]

    # =====================================================================
    # 7.5 پاک‌سازی زمان‌بندی: حذف رکوردهای ثانیه‌ای، حذف زمان‌های تکراری و
    #      پر کردن فاصله‌های خالی با فرکانس دقیقه‌ای ثابت
    # این مرحله اطمینان می‌دهد که خروجی بدون ثانیه اضافی و با فواصل زمانی
    # یک‌دقیقه‌ای یکنواخت باشد. همچنین زمان‌های تکراری (timestamp یکسان)
    # حذف می‌شود و مقادیر سنسورها در زمان‌های جدید با درون‌یابی خطی پر
    # می‌گردد و شناسه چاه و کلاس با روش forward/backward پر می‌شود.
    print("\n🧹 پاک‌سازی زمان‌بندی...")
    # تبدیل timestamp به datetime برای پردازش
    df['timestamp_dt'] = pd.to_datetime(df['timestamp'])
    # حذف رکوردهایی که ثانیه آن‌ها صفر نیست (ثانیه‌ای بودن)
    before_seconds = len(df)
    df = df[df['timestamp_dt'].dt.second == 0].copy()
    removed_seconds = before_seconds - len(df)
    if removed_seconds > 0:
        print(f"   ✓ {removed_seconds} رکورد با ثانیه غیر صفر حذف شد")
    else:
        print("   ✓ رکورد ثانیه‌ای یافت نشد")
    # حذف زمان‌های تکراری (keep first)
    before_dup = len(df)
    df = df.drop_duplicates(subset=['timestamp_dt'], keep='first')
    removed_dups = before_dup - len(df)
    if removed_dups > 0:
        print(f"   ✓ {removed_dups} رکورد زمان تکراری حذف شد")
    else:
        print("   ✓ زمان تکراری یافت نشد")
    # پر کردن فواصل خالی
    # دامنه زمانی کامل بین اولین و آخرین زمان موجود با دقت 1 دقیقه
    start = df['timestamp_dt'].min()
    end = df['timestamp_dt'].max()
    full_range = pd.date_range(start=start, end=end, freq='1min')
    # Reindex بر اساس timestamp_dt
    df = df.set_index('timestamp_dt').reindex(full_range)
    # پس از reindex، index همان timestamp جدید است
    df['timestamp_dt'] = df.index
    # پر کردن کلاس و well_id با forward/backward fill
    if 'class' in df.columns:
        df['class'] = df['class'].ffill().bfill()
    if 'well_id' in df.columns:
        df['well_id'] = df['well_id'].ffill().bfill()
    # درون‌یابی خطی برای سنسورها
    for col in sensor_cols:
        if col in ['timestamp', 'original_timestamp', '_old_timestamp']:
            continue
        # فقط برای ستون‌های عددی اعمال شود
        if col in df.columns:
            df[col] = df[col].interpolate(method='linear', limit_direction='both')
    # ریست index
    df = df.reset_index(drop=True)
    # تبدیل timestamp به string
    df['timestamp'] = df['timestamp_dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df = df.drop(columns=['timestamp_dt'])

    # 9. ذخیره
    print("\n💾 ذخیره فایل...")
    df.to_json(output_file, orient='records', indent=2, force_ascii=False)
    print(f"   ✓ {output_file}")

    # 10. گزارش نهایی
    print("\n" + "=" * 80)
    print("📊 گزارش نهایی:")
    print("=" * 80)
    print(f"✓ تعداد رکوردها: {len(df):,}")
    print(f"✓ تعداد ستون‌ها: {len(df.columns)}")
    print(f"✓ بازه زمانی: {df['timestamp'].min()} تا {df['timestamp'].max()}")
    print(f"✓ فاصله زمانی: 1 دقیقه (minute-by-minute)")
    print(f"✓ NULL های باقیمانده: {df.isna().sum().sum()}")
    print(f"✓ کلاس‌ها: {sorted(df['class'].unique())}")
    print(f"✓ فایل خروجی: {output_file}")
    print(f"✓ حجم: {len(df.to_json(orient='records'))/1024:.1f} KB")

    print("\n" + "=" * 80)
    print("✅ پیش‌پردازش با موفقیت کامل شد!")
    print("=" * 80)

    return df

if __name__ == "__main__":
    df = preprocess_data(
        input_file='MData.json',
        output_file='MData_Cleaned.json'
    )

    print("\n💡 نکته: اکنون می‌توانید app_final.py را اجرا کنید")
