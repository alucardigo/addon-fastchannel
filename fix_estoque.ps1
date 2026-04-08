$path = "x:\IntegracaoFastchannel\APPFASTCHANNEL\.worktrees\merge-unify\vc\src\main\webapp\html5\fastchannel\estoque.html"
$content = Get-Content $path -Raw
$content = $content -replace '(?s)        \.filters \{.*?margin-bottom: 20px;\s*\}', "        .filters {`r`n            display: flex;`r`n            gap: 16px;`r`n            flex-wrap: wrap;`r`n            margin-bottom: 24px;`r`n        }"
$content = $content -replace '(?s)        \.filter-group \{.*?gap: 4px;\s*\}', "        .filter-group {`r`n            display: flex;`r`n            flex-direction: column;`r`n            gap: 4px;`r`n            flex: 1;`r`n            min-width: 150px;`r`n        }"
$content = $content -replace '(?s)        \.btn \{.*?gap: 8px;\s*\}', "        .btn {`r`n            padding: 10px 20px;`r`n            border: none;`r`n            border-radius: 8px;`r`n            font-size: 14px;`r`n            font-weight: 500;`r`n            cursor: pointer;`r`n            transition: all 0.2s;`r`n            display: inline-flex;`r`n            align-items: center;`r`n            justify-content: center;`r`n            gap: 8px;`r`n        }"
Set-Content $path -Value $content -NoNewline
