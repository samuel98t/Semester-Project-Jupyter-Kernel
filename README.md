### A Python / Julia Dual Kernel.
A jupyter notebook kernel made for an easy quick mix of both julia and python in the same jupyter notebook, julia interactions made possible by the use of juliacall . and kernel communcation with the front-end made possible by the use of zmq .

### Features :
- Dual language support by using the correct magics at the top of the cell. (**%python**/**%julia**)
- Inline prints/errors (stdout/stderr) in the notebook.
- Input taking.
- Basic Interrupt support.
- Inline Display support.
In Python using display() for a supported (_repr_html/png/jpeg/svg).
In Julia using display("mime_bundle",data) where mime_bundle can be "text/html","image/svg+xml","image/png","image/jpeg".
Use examples are in the Testing.ipynb notebook.
- Autocomplete using TAB.
- Variable sharing between languages using the **%jl2py** / **%py2jl** magics.
**Example** : %jl2py x,y,z

### How to Install and use:
- Download the 3 files : **kernel.json**,**my_kernel.py** and **requirements.txt** to the same folder.
- Create and activate python enviorment:
- python -m venv venv (or python3)
- venv\Scripts\activate.bat
- Install Requirments
- pip install -r requirements.txt
Now to install the kernel:
- jupyter kernelspec installkernel_dir --name=my_kernel
- verify with jupyter kernelspec list
- Now open jupyter notebook by typing jupyter notebook in the terminal.
- Create a notebook and select the newly installed kernel .

  ### FOR EXAMPLES CHECK Testing.ipynb
