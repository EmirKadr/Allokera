# main.py
import tkinter as tk
from allokera.gui.app import App
from allokera.config.constants import APP_TITLE

def main():
    root = tk.Tk()
    root.title(APP_TITLE)
    app = App(root)
    root.mainloop()

if __name__ == "__main__":
    main()
