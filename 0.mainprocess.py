import ctypes
import time
import subprocess

# 防止電腦進入睡眠模式
ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001

# 使用 ctypes 與 Windows API 交互
ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)

# 要執行的6個程式的路徑
scripts = [
    r'G:\我的雲端硬碟\Horizon\Python\1.stock_code_IEPSQALPWC_combine.py',
    r'G:\我的雲端硬碟\Horizon\Python\2A.ipo_sauron_AATEDRADTANLSCM_close.py',
    r'G:\我的雲端硬碟\Horizon\Python\2B.ipo_sauron_AATEDRADTANLSCM_close.py',
    r'G:\我的雲端硬碟\Horizon\Python\2C.ipo_sauron_AATEDRADTANLSCM_close.py',
    r'G:\我的雲端硬碟\Horizon\Python\2D.ipo_sauron_AATEDRADTANLSCM_close.py',
    r'G:\我的雲端硬碟\Horizon\Python\3.calculation_4Q_PMDDMSRRNOIPSBSC_close.py'
]

# 依序執行每個程式，並且每次執行完後等待6分鐘
for index, script in enumerate(scripts):
    print(f"正在執行 {script}...")
    subprocess.run(['python', script])
    print(f"{script} 執行完畢")
    
    # 只有在不是最後一個腳本的時候才等待6分鐘
    if index < len(scripts) - 1:
        print("等待6分鐘...")
        time.sleep(6 * 60)  # 6分鐘 = 6 * 60秒

print("所有程式已執行完畢。")

# 恢復正常的電源設定
ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)
