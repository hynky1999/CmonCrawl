docker-compose down --remove-orphans
docker volume rm -f rocnikovyprojekt_artemis-data
docker-compose up --build