import requests
import json

# API端点
url = "http://127.0.0.1:8000/api/stocks/update/index_components/all"

print("测试获取所有指数列表API...")

# 发送GET请求
try:
    response = requests.get(url)
    
    # 检查响应状态码
    if response.status_code == 200:
        # 解析响应数据
        data = response.json()
        
        print(f"请求成功，状态码: {response.status_code}")
        print(f"完整响应: {json.dumps(data, ensure_ascii=False, indent=2)}")
        
        # 打印指数列表
        indices = data.get('data', [])
        print(f"获取到 {len(indices)} 个指数")
        
    else:
        print(f"请求失败，状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        
except Exception as e:
    print(f"请求异常: {str(e)}")
    import traceback
    traceback.print_exc()
