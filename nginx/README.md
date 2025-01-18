# Nginx installation

```sh
sudo apt install -y nginx certbot python3-certbot-nginx
```

# Set up nginx for Metabase

```sh
sudo vim /etc/nginx/sites-available/metabase.rdxz2.site  # Copy the content of metabase.rdxz2.site
sudo ln -s /etc/nginx/sites-available/metabase.rdxz2.site /etc/nginx/sites-enabled/metabase.rdxz2.site
sudo nginx -t
sudo systemctl reload nginx

sudo certbot --nginx  # Select the metabase.rdxz2.site
```

# Set up nginx for Airflow

```sh
sudo vim /etc/nginx/sites-available/airflow.rdxz2.site  # Copy the content of airflow.rdxz2.site
sudo ln -s /etc/nginx/sites-available/airflow.rdxz2.site /etc/nginx/sites-enabled/airflow.rdxz2.site
sudo nginx -t
sudo systemctl reload nginx

sudo certbot --nginx  # Select the airflow.rdxz2.site
```

# Set up nginx for Cloudbeaver

```sh
sudo vim /etc/nginx/sites-available/cloudbeaver.rdxz2.site  # Copy the content of cloudbeaver.rdxz2.site
sudo ln -s /etc/nginx/sites-available/cloudbeaver.rdxz2.site /etc/nginx/sites-enabled/cloudbeaver.rdxz2.site
sudo nginx -t
sudo systemctl reload nginx

sudo certbot --nginx  # Select the cloudbeaver.rdxz2.site
```
