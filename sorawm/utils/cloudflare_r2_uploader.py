"""
Cloudflare R2 文件上传工具
支持单文件上传、批量上传、进度显示等功能
"""

import os
import time
import hashlib
from typing import Optional, Dict, Any, List, Callable
from pathlib import Path
import mimetypes
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config
import requests
from tqdm import tqdm


class CloudflareR2Uploader:
    """Cloudflare R2 文件上传器"""
    
    def __init__(
        self,
        account_id: str,
        access_key_id: str,
        secret_access_key: str,
        bucket_name: str,
        region: str = "auto",
        endpoint_url: Optional[str] = None
    ):
        """
        初始化 R2 上传器
        
        Args:
            account_id: Cloudflare 账户 ID
            access_key_id: R2 API Token 的 Access Key ID
            secret_access_key: R2 API Token 的 Secret Access Key
            bucket_name: R2 存储桶名称
            region: 区域，通常为 "auto"
            endpoint_url: 自定义端点 URL
        """
        self.account_id = account_id
        self.bucket_name = bucket_name
        self.region = region
        
        # 构建端点 URL
        if endpoint_url is None:
            endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
        
        # 配置 boto3 客户端
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region,
            config=Config(
                signature_version='s3v4',
                s3={
                    'addressing_style': 'path'
                }
            )
        )
        
        # 验证连接
        self._verify_connection()
    
    def _verify_connection(self) -> None:
        """验证 R2 连接"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"✅ 成功连接到 R2 存储桶: {self.bucket_name}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                raise Exception(f"存储桶 '{self.bucket_name}' 不存在")
            elif error_code == '403':
                raise Exception(f"没有访问存储桶 '{self.bucket_name}' 的权限")
            else:
                raise Exception(f"连接 R2 失败: {e}")
        except NoCredentialsError:
            raise Exception("R2 凭据无效或缺失")
    
    def _get_content_type(self, file_path: str) -> str:
        """获取文件 MIME 类型"""
        content_type, _ = mimetypes.guess_type(file_path)
        return content_type or 'application/octet-stream'
    
    def _calculate_file_hash(self, file_path: str) -> str:
        """计算文件 MD5 哈希值"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def _create_progress_callback(self, file_size: int, filename: str) -> Callable:
        """创建上传进度回调函数"""
        progress_bar = tqdm(
            total=file_size,
            unit='B',
            unit_scale=True,
            desc=f"上传 {filename}",
            ncols=80
        )
        
        def callback(bytes_transferred):
            progress_bar.update(bytes_transferred)
        
        return callback, progress_bar
    
    def upload_file(
        self,
        local_file_path: str,
        remote_key: Optional[str] = None,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        show_progress: bool = True,
        overwrite: bool = True
    ) -> Dict[str, Any]:
        """
        上传单个文件到 R2
        
        Args:
            local_file_path: 本地文件路径
            remote_key: 远程文件键名，如果为 None 则使用文件名
            content_type: 文件 MIME 类型，如果为 None 则自动检测
            metadata: 文件元数据
            show_progress: 是否显示上传进度
            overwrite: 是否覆盖已存在的文件
            
        Returns:
            上传结果信息
        """
        # 验证文件存在
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"文件不存在: {local_file_path}")
        
        # 获取文件信息
        file_path = Path(local_file_path)
        file_size = file_path.stat().st_size
        
        # 设置远程键名
        if remote_key is None:
            remote_key = file_path.name
        
        # 检查文件是否已存在
        if not overwrite:
            try:
                self.s3_client.head_object(Bucket=self.bucket_name, Key=remote_key)
                raise FileExistsError(f"文件已存在: {remote_key}")
            except ClientError as e:
                if e.response['Error']['Code'] != '404':
                    raise
        
        # 设置内容类型
        if content_type is None:
            content_type = self._get_content_type(local_file_path)
        
        # 准备上传参数
        upload_kwargs = {
            'Bucket': self.bucket_name,
            'Key': remote_key,
            'ContentType': content_type
        }
        
        # 添加元数据
        if metadata:
            upload_kwargs['Metadata'] = metadata
        
        # 添加文件哈希
        file_hash = self._calculate_file_hash(local_file_path)
        upload_kwargs['Metadata'] = upload_kwargs.get('Metadata', {})
        upload_kwargs['Metadata']['file-hash'] = file_hash
        
        try:
            if show_progress and file_size > 1024 * 1024:  # 大于 1MB 显示进度
                callback, progress_bar = self._create_progress_callback(file_size, file_path.name)
                
                self.s3_client.upload_file(
                    local_file_path,
                    self.bucket_name,
                    remote_key,
                    ExtraArgs={
                        'ContentType': content_type,
                        'Metadata': upload_kwargs['Metadata']
                    },
                    Callback=callback
                )
                progress_bar.close()
            else:
                self.s3_client.upload_file(
                    local_file_path,
                    self.bucket_name,
                    remote_key,
                    ExtraArgs={
                        'ContentType': content_type,
                        'Metadata': upload_kwargs['Metadata']
                    }
                )
            
            # 获取上传后的文件信息
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=remote_key)
            
            result = {
                'success': True,
                'local_path': local_file_path,
                'remote_key': remote_key,
                'file_size': file_size,
                'content_type': content_type,
                'etag': response.get('ETag', '').strip('"'),
                'last_modified': response.get('LastModified'),
                'metadata': response.get('Metadata', {}),
                'url': f"https://{self.bucket_name}.{self.account_id}.r2.cloudflarestorage.com/{remote_key}"
            }
            
            print(f"✅ 文件上传成功: {remote_key}")
            return result
            
        except ClientError as e:
            error_msg = f"上传文件失败: {e}"
            print(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'local_path': local_file_path,
                'remote_key': remote_key
            }
    
    def upload_multipart(
        self,
        local_file_path: str,
        remote_key: Optional[str] = None,
        part_size: int = 5 * 1024 * 1024,  # 5MB
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        show_progress: bool = True
    ) -> Dict[str, Any]:
        """
        使用分片上传大文件
        
        Args:
            local_file_path: 本地文件路径
            remote_key: 远程文件键名
            part_size: 分片大小（字节）
            content_type: 文件 MIME 类型
            metadata: 文件元数据
            show_progress: 是否显示进度
            
        Returns:
            上传结果信息
        """
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"文件不存在: {local_file_path}")
        
        file_path = Path(local_file_path)
        file_size = file_path.stat().st_size
        
        if remote_key is None:
            remote_key = file_path.name
        
        if content_type is None:
            content_type = self._get_content_type(local_file_path)
        
        # 准备上传参数
        upload_kwargs = {
            'Bucket': self.bucket_name,
            'Key': remote_key,
            'ContentType': content_type
        }
        
        if metadata:
            upload_kwargs['Metadata'] = metadata
        
        try:
            # 创建分片上传
            response = self.s3_client.create_multipart_upload(**upload_kwargs)
            upload_id = response['UploadId']
            
            # 计算分片数量
            num_parts = (file_size + part_size - 1) // part_size
            
            parts = []
            progress_bar = None
            
            if show_progress:
                progress_bar = tqdm(
                    total=file_size,
                    unit='B',
                    unit_scale=True,
                    desc=f"分片上传 {file_path.name}",
                    ncols=80
                )
            
            try:
                with open(local_file_path, 'rb') as file:
                    for part_num in range(1, num_parts + 1):
                        # 读取分片数据
                        chunk = file.read(part_size)
                        
                        # 上传分片
                        part_response = self.s3_client.upload_part(
                            Bucket=self.bucket_name,
                            Key=remote_key,
                            PartNumber=part_num,
                            UploadId=upload_id,
                            Body=chunk
                        )
                        
                        parts.append({
                            'ETag': part_response['ETag'],
                            'PartNumber': part_num
                        })
                        
                        if progress_bar:
                            progress_bar.update(len(chunk))
                
                # 完成分片上传
                self.s3_client.complete_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=remote_key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )
                
                if progress_bar:
                    progress_bar.close()
                
                result = {
                    'success': True,
                    'local_path': local_file_path,
                    'remote_key': remote_key,
                    'file_size': file_size,
                    'content_type': content_type,
                    'upload_type': 'multipart',
                    'parts_count': num_parts,
                    'url': f"https://{self.bucket_name}.{self.account_id}.r2.cloudflarestorage.com/{remote_key}"
                }
                
                print(f"✅ 分片上传成功: {remote_key} ({num_parts} 个分片)")
                return result
                
            except Exception as e:
                # 取消分片上传
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=remote_key,
                    UploadId=upload_id
                )
                raise e
                
        except ClientError as e:
            error_msg = f"分片上传失败: {e}"
            print(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'local_path': local_file_path,
                'remote_key': remote_key
            }
    
    def upload_directory(
        self,
        local_dir_path: str,
        remote_prefix: str = "",
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        show_progress: bool = True
    ) -> Dict[str, Any]:
        """
        上传整个目录
        
        Args:
            local_dir_path: 本地目录路径
            remote_prefix: 远程前缀
            include_patterns: 包含的文件模式
            exclude_patterns: 排除的文件模式
            show_progress: 是否显示进度
            
        Returns:
            上传结果统计
        """
        if not os.path.isdir(local_dir_path):
            raise NotADirectoryError(f"目录不存在: {local_dir_path}")
        
        dir_path = Path(local_dir_path)
        results = {
            'success': 0,
            'failed': 0,
            'total': 0,
            'files': []
        }
        
        # 收集所有文件
        all_files = []
        for file_path in dir_path.rglob('*'):
            if file_path.is_file():
                # 检查包含模式
                if include_patterns:
                    if not any(file_path.match(pattern) for pattern in include_patterns):
                        continue
                
                # 检查排除模式
                if exclude_patterns:
                    if any(file_path.match(pattern) for pattern in exclude_patterns):
                        continue
                
                all_files.append(file_path)
        
        results['total'] = len(all_files)
        
        if show_progress:
            file_progress = tqdm(all_files, desc="上传目录", ncols=80)
        else:
            file_progress = all_files
        
        for file_path in file_progress:
            # 计算相对路径
            relative_path = file_path.relative_to(dir_path)
            remote_key = f"{remote_prefix}/{relative_path}".lstrip('/')
            
            try:
                result = self.upload_file(
                    str(file_path),
                    remote_key,
                    show_progress=False
                )
                
                if result['success']:
                    results['success'] += 1
                else:
                    results['failed'] += 1
                
                results['files'].append(result)
                
            except Exception as e:
                results['failed'] += 1
                results['files'].append({
                    'success': False,
                    'error': str(e),
                    'local_path': str(file_path),
                    'remote_key': remote_key
                })
        
        print(f"📁 目录上传完成: 成功 {results['success']}/{results['total']} 个文件")
        return results
    
    def get_file_url(self, remote_key: str, expires_in: int = 3600) -> str:
        """
        获取文件的预签名 URL
        
        Args:
            remote_key: 远程文件键名
            expires_in: URL 过期时间（秒）
            
        Returns:
            预签名 URL
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': remote_key},
                ExpiresIn=expires_in
            )
            return url
        except ClientError as e:
            raise Exception(f"生成预签名 URL 失败: {e}")
    
    def delete_file(self, remote_key: str) -> bool:
        """
        删除文件
        
        Args:
            remote_key: 远程文件键名
            
        Returns:
            是否删除成功
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=remote_key)
            print(f"🗑️ 文件删除成功: {remote_key}")
            return True
        except ClientError as e:
            print(f"❌ 文件删除失败: {e}")
            return False
    
    def list_files(self, prefix: str = "", max_keys: int = 1000) -> List[Dict[str, Any]]:
        """
        列出文件
        
        Args:
            prefix: 文件前缀
            max_keys: 最大返回数量
            
        Returns:
            文件列表
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            files = []
            for obj in response.get('Contents', []):
                files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'],
                    'etag': obj['ETag'].strip('"'),
                    'url': f"https://{self.bucket_name}.{self.account_id}.r2.cloudflarestorage.com/{obj['Key']}"
                })
            
            return files
        except ClientError as e:
            raise Exception(f"列出文件失败: {e}")
"""token YQCXJt27x1jIfEKlRGTrs733jZtONayZK080wnH7 """

def main():
    """示例用法"""
    # 配置信息（请替换为您的实际配置）
    config = {
        'account_id': 'ff4dc1637dea73f687c64ec9d2f17009',
        'access_key_id': 'b60ebd3d363f4a31d5b836f0d888f308',
        'secret_access_key': 'dcdef3ae6d52b7a8442a9fbc53cccc0f56c227407fda25ee86534ded8e3991de',
        'bucket_name': 'sora'
    }
    
    try:
        # 创建上传器
        uploader = CloudflareR2Uploader(**config)
        
        # 示例 1: 上传单个文件
        # print("=== 上传单个文件 ===")
        # result = uploader.upload_file(
        #     'resources/puppies.mp4',
        #     'uploads/puppies.mp4',
        #     metadata={'author': 'python_script', 'version': '1.0'}
        # )
        # print(f"上传结果: {result}")
        
        # # 示例 2: 分片上传大文件
        print("\n=== 分片上传大文件 ===")
        # result = uploader.upload_multipart(
        #     '../../resources/puppies.mp4',
        #     'uploads/puppies.mp4',
        #     part_size=10 * 1024 * 1024  # 10MB 分片
        # )
        # print(f"分片上传结果: {result}")
        #
        # # 示例 3: 上传目录
        # print("\n=== 上传目录 ===")
        # result = uploader.upload_directory(
        #     './uploads',
        #     'backup/',
        #     include_patterns=['*.txt', '*.jpg'],
        #     exclude_patterns=['*.tmp']
        # )
        # print(f"目录上传结果: {result}")
        
        # 示例 4: 获取文件 URL
        print("\n=== 获取文件 URL ===")
        url = uploader.get_file_url('uploads/puppies.mp4', expires_in=7200)
        print(f"文件 URL: {url}")
        
        # 示例 5: 列出文件
        print("\n=== 列出文件 ===")
        files = uploader.list_files('uploads/', max_keys=10)
        for file_info in files:
            print(f"文件: {file_info['key']}, 大小: {file_info['size']} bytes")
        
    except Exception as e:
        print(f"❌ 错误: {e}")




if __name__ == "__main__":
    main()

