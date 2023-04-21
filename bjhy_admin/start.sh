export PATH=/home/toptrade/anaconda3/envs/selpy39/bin:$PATH
nohup python manage.py runserver 192.168.101.218:8000 --noreload > /dev/null 2>&1 &
echo "python manage.py runserver 192.168.101.218:8000 --noreload ...."