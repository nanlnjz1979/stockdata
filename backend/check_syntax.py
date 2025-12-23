import os
import sys

def check_python_syntax(directory):
    """检查指定目录下所有Python文件的语法错误"""
    has_error = False
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        compile(f.read(), file_path, 'exec')
                except SyntaxError as e:
                    print(f"Syntax error in {file_path}:")
                    print(f"  Line {e.lineno}, Column {e.offset}: {e.msg}")
                    print(f"  {e.text.strip()}")
                    has_error = True
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
                    has_error = True
    return has_error

if __name__ == "__main__":
    current_dir = os.getcwd()
    has_error = check_python_syntax(current_dir)
    sys.exit(1 if has_error else 0)
