sudo systemctl enable fileuploadstrigger --now
sudo systemctl start fileuploadstrigger
sudo systemctl daemon-reload
crontab ./cron_job_schedules.txt