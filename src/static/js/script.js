// ========== 기존 API 테스트 함수 ==========

function testAPI() {
    fetch('/mails?title=테스트메일')
        .then(res => res.json())
        .then(data => {
            document.getElementById('response').textContent = JSON.stringify(data, null, 2);
        })
        .catch(err => {
            document.getElementById('response').textContent = '에러: ' + err.message;
        });
}

function fetchData() {
    fetch('/api/graph-data')
        .then(res => res.json())
        .then(data => {
            document.getElementById('data-response').textContent = JSON.stringify(data, null, 2);
        })
        .catch(err => {
            document.getElementById('data-response').textContent = '에러: ' + err.message;
        });
}

function sendName() {
    const name = document.getElementById('nameInput').value;
    if (name.trim()) {
        document.getElementById('name-response').textContent = `안녕하세요, ${name}님!`;
    } else {
        document.getElementById('name-response').textContent = '이름을 입력해주세요.';
    }
}
