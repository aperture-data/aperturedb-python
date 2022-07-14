const request = (query, blobs, handler, sessionToken) => {
    apiURL =  "https://coco.datasets.aperturedata.io/api"
    const formData = new FormData();
    formData.append('query', JSON.stringify(query));
    displayContent(query, response=false);
    
    blobs.forEach(element => {
        formData.append('blobs', element);
    });

    let headers = null;
    if (sessionToken != null){
        console.log(`setting session token ${sessionToken}`);
        headers = {
            "Authorization": `Bearer ${sessionToken}`
        }
    }
    
    axios.post(
            url=apiURL,
            data=formData, {
                headers: headers
            }).then((response)=>{
                handler(response.data)
            })
}

const displayContent = (payload, response=true) => {
    var tag = document.createElement("p");
    var text = JSON.stringify(payload, undefined, 4);
    var element = document.getElementById("output");
    var br = document.createElement("hr");
    prefix = response ? "<<<<<<< Response" : "Request >>>>>>>";
    tag.innerHTML = `${prefix}\r\n${text}`;
    element.appendChild(tag);
    element.appendChild(br);
}

run_requests = () => {
    //Get a refresh token.
    auth = [{
        "Authenticate": {
            "username": "admin",
            "password": "admin"
        }
    }]
    request(query = auth, blobs = [], handler = (data)=>{
        authData = data["json"];
        // console.log(authData[0]);
        displayContent(authData);
        sessionToken = authData[0].Authenticate.session_token;
        

        //List images
        listQuery = [{
            "FindImage": {
                "blobs": false,
                "uniqueids": true,
                "results" : {
                    "limit": 10
                }
            }
        }]
        request(query = listQuery, blobs = [], handler = (data) => {
            response = data["json"];
            displayContent(response);

            //Find an image
            findQuery = [{
                "FindImage": {
                    "constraints": {
                        "_uniqueid": ["==", response[0].FindImage.entities[0]._uniqueid]
                    },
                    "results": {
                        "all_properties": true
                    }
                }
            }]
            request(query = findQuery, blobs = [], handler = (data) => {
                response = data["json"];
                console.log(data);
                displayContent(response);
                const url = `data:image/jpeg;base64,${data["blobs"][0]}`;
                fetch(url)
                .then(res=>res.blob())
                .then(blob=>{
                    var image = document.createElement('img');
                    console.log(blob);
                    image.src = window.webkitURL.createObjectURL(blob);
                    var element = document.getElementById("output");
                    element.appendChild(image);
                });
            }, sessionToken = sessionToken)


        }, sessionToken=sessionToken)

        sessionStorage.setItem("session_token", sessionToken);
    })

    
    
}

const addImage = (event) => {
    event.preventDefault();
    query = [{
        "AddImage": {
            "properties": {
                "rest_api_example_id": 123456789
            }
        }
    }];
    const file = document.getElementById("fileupload").files[0]
    request(query = query, blobs = [file], (data)=>{
        response = data["json"];
        displayContent(response);
    }, sessionToken = sessionStorage.getItem("session_token"));
    
}

window.addEventListener("load", (event)=>{
    console.log("hello world");
    const form = document.getElementById("addimage");
    form.addEventListener('submit', addImage);
})
