# Installation
- `docker-compose up -d`
- run `init.sql` (see [useful_command](useful_command.txt))
- connect clickstack and grafana:
    - grafana username and pass: admin
    - install clickhouse plugin
    - add data source
        - server: clickstack
        - port: 8123
        - protocol: http
        - user: grafana
        - pass: 123
		
- access nifi:
	- url: https://localhost:8443/nifi
	- username: admin
	- password: Password1234
