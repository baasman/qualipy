from qualipy.web.app import create_app

server = create_app()

if __name__ == "__main__":
    server.run(debug=True)
