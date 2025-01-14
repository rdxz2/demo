import os

parent_dir = os.path.join(os.path.dirname(__file__), '..')


for root, _, files in os.walk(parent_dir):
    for file in files:
        if file.startswith('.env') and not file.endswith('.template'):
            env_file = file
            env_template_file = f'{file}.template'

            env_file = os.path.join(root, env_file)
            env_template_file = os.path.join(root, env_template_file)

            print(f'Processing {env_file} -> {env_template_file}')

            with open(env_file, 'r') as f:
                with open(env_template_file, 'w') as f_template:
                    while content := f.readline():
                        content = content.split('=')[0]
                        if content == '\n':
                            content = ''
                        elif content.startswith('#'):
                            content = content.removesuffix('\n')
                        else:
                            content = content + '='
                        f_template.write(content + '\n')
