# Nginx installation

```sh
sudo apt install -y nginx certbot python3-certbot-nginx
```

# Set up nginx for Metabase

```sh
sudo vim /etc/nginx/sites-available/metabase.rdxz2.site  # Copy the content of metabase.rdxz2.site
sudo nginx -t
sudo systemctl reload nginx

sudo certbot --nginx  # Select the metabase.rdxz2.site
```

# Set up nginx for Airflow

```sh
sudo vim /etc/nginx/sites-available/airflow.rdxz2.site  # Copy the content of airflow.rdxz2.site
sudo nginx -t
sudo systemctl reload nginx

sudo certbot --nginx  # Select the airflow.rdxz2.site
```
