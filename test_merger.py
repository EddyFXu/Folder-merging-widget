import unittest
import os
import shutil
import tempfile
import sys

# 确保能导入 folder_merger
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from folder_merger import MergerCore

class TestMerger(unittest.TestCase):
    def setUp(self):
        # 创建临时目录结构
        self.test_dir = tempfile.mkdtemp()
        self.source_dir = os.path.join(self.test_dir, "source")
        self.target_dir = os.path.join(self.test_dir, "target")
        os.makedirs(self.source_dir)
        os.makedirs(self.target_dir)
        
        # 创建测试文件
        # source/file1.txt
        with open(os.path.join(self.source_dir, "file1.txt"), "w") as f: f.write("content1")
        
        # source/sub1/file2.txt
        os.makedirs(os.path.join(self.source_dir, "sub1"))
        with open(os.path.join(self.source_dir, "sub1", "file2.txt"), "w") as f: f.write("content2")
        
        # source/sub2/file1.txt (重名文件)
        os.makedirs(os.path.join(self.source_dir, "sub2"))
        with open(os.path.join(self.source_dir, "sub2", "file1.txt"), "w") as f: f.write("content3_collision")

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_basic_copy(self):
        core = MergerCore()
        config = {
            'source_dir': self.source_dir,
            'target_parent': self.target_dir,
            'files_per_folder': 100,
            'operation_mode': 'copy',
            'rename_mode': 'keep',
            'conflict_mode': 'auto_rename',
            'custom_prefix': ''
        }
        core.process(config)
        
        # 验证结果
        target_subdir = os.path.join(self.target_dir, "Merged_1")
        self.assertTrue(os.path.exists(target_subdir))
        
        files = os.listdir(target_subdir)
        self.assertEqual(len(files), 3)
        self.assertIn("file1.txt", files)
        self.assertIn("file2.txt", files)
        # 检查重名文件是否被重命名
        collision_files = [f for f in files if f.startswith("file1 (")]
        self.assertEqual(len(collision_files), 1)

    def test_split_folders(self):
        core = MergerCore()
        config = {
            'source_dir': self.source_dir,
            'target_parent': self.target_dir,
            'files_per_folder': 2, # 每文件夹 2 个
            'operation_mode': 'copy',
            'rename_mode': 'keep',
            'conflict_mode': 'auto_rename',
            'custom_prefix': ''
        }
        core.process(config)
        
        self.assertTrue(os.path.exists(os.path.join(self.target_dir, "Merged_1")))
        self.assertTrue(os.path.exists(os.path.join(self.target_dir, "Merged_2")))
        
        files1 = os.listdir(os.path.join(self.target_dir, "Merged_1"))
        files2 = os.listdir(os.path.join(self.target_dir, "Merged_2"))
        
        self.assertEqual(len(files1) + len(files2), 3)

    def test_move_operation(self):
        core = MergerCore()
        config = {
            'source_dir': self.source_dir,
            'target_parent': self.target_dir,
            'files_per_folder': 100,
            'operation_mode': 'move',
            'rename_mode': 'keep',
            'conflict_mode': 'auto_rename',
            'custom_prefix': ''
        }
        core.process(config)
        
        target_subdir = os.path.join(self.target_dir, "Merged_1")
        self.assertEqual(len(os.listdir(target_subdir)), 3)
        
        # 验证源文件已被移动
        self.assertFalse(os.path.exists(os.path.join(self.source_dir, "file1.txt")))
        
        # 验证源文件夹已被清理（sub1 应该没了，因为它是空的）
        self.assertFalse(os.path.exists(os.path.join(self.source_dir, "sub1")))

    def test_rename_parent(self):
        core = MergerCore()
        config = {
            'source_dir': self.source_dir,
            'target_parent': self.target_dir,
            'files_per_folder': 100,
            'operation_mode': 'copy',
            'rename_mode': 'parent_name',
            'conflict_mode': 'auto_rename',
            'custom_prefix': ''
        }
        core.process(config)
        
        target_subdir = os.path.join(self.target_dir, "Merged_1")
        files = os.listdir(target_subdir)
        
        # 根目录下的文件，父目录名就是 source_dir 的名字 (即 "source")
        self.assertIn("source_file1.txt", files)
        # 子目录下的文件
        self.assertIn("sub1_file2.txt", files)
        self.assertIn("sub2_file1.txt", files)

    def test_resume_merge(self):
        """测试断点续传/接续合并"""
        # 1. 预先创建 Merged_1 并放入一个文件
        merged_1 = os.path.join(self.target_dir, "Merged_1")
        os.makedirs(merged_1)
        with open(os.path.join(merged_1, "existing.txt"), "w") as f: f.write("old")
        
        core = MergerCore()
        config = {
            'source_dir': self.source_dir,
            'target_parent': self.target_dir,
            'files_per_folder': 2, # 限制为 2
            'operation_mode': 'copy',
            'rename_mode': 'keep',
            'conflict_mode': 'auto_rename',
            'custom_prefix': ''
        }
        # Merged_1 已有 1 个文件，限制 2。
        # 应该再往 Merged_1 放 1 个文件，然后创建 Merged_2
        
        core.process(config)
        
        files1 = os.listdir(merged_1)
        self.assertEqual(len(files1), 2) # existing + 1 new
        self.assertIn("existing.txt", files1)
        
        merged_2 = os.path.join(self.target_dir, "Merged_2")
        self.assertTrue(os.path.exists(merged_2))
        files2 = os.listdir(merged_2)
        self.assertEqual(len(files2), 2) # 剩下 2 个 new

    def test_timely_cleanup(self):
        """测试及时清理空文件夹"""
        # 使用 move 模式
        # 构造深层结构 source/deep/nested/file.txt
        deep_dir = os.path.join(self.source_dir, "deep", "nested")
        os.makedirs(deep_dir)
        with open(os.path.join(deep_dir, "file.txt"), "w") as f: f.write("content")
        
        core = MergerCore()
        config = {
            'source_dir': self.source_dir,
            'target_parent': self.target_dir,
            'files_per_folder': 100,
            'operation_mode': 'move',
            'rename_mode': 'keep',
            'conflict_mode': 'auto_rename',
            'custom_prefix': ''
        }
        core.process(config)
        
        # 验证 deep/nested 已经被删除
        self.assertFalse(os.path.exists(deep_dir))
        self.assertFalse(os.path.exists(os.path.join(self.source_dir, "deep")))

    def test_fill_gaps_and_partials(self):
        """测试填充空缺文件夹和不完整文件夹"""
        # 场景:
        # Merged_1: 2个文件 (限制3) -> 需要填1个
        # Merged_2: 缺失 -> 需要创建并填3个
        # Merged_3: 1个文件 (限制3) -> 需要填2个
        # 源文件: 10个文件
        # 预期:
        # Merged_1: +1 = 3 (满)
        # Merged_2: +3 = 3 (满)
        # Merged_3: +2 = 3 (满)
        # Merged_4: +3 = 3 (满, 新建)
        # Merged_5: +1 = 1 (剩余, 新建)

        # 1. 准备环境
        m1 = os.path.join(self.target_dir, "Merged_1")
        os.makedirs(m1)
        for i in range(2):
            with open(os.path.join(m1, f"old_1_{i}.txt"), "w") as f: f.write("content")
        
        # 跳过 Merged_2
        
        m3 = os.path.join(self.target_dir, "Merged_3")
        os.makedirs(m3)
        with open(os.path.join(m3, "old_3_0.txt"), "w") as f: f.write("content")
        
        # 准备源文件 (10个)
        for i in range(10):
            with open(os.path.join(self.source_dir, f"new_{i}.txt"), "w") as f: f.write("new content")

        # 2. 执行合并
        core = MergerCore()
        config = {
            'source_dir': self.source_dir,
            'target_parent': self.target_dir,
            'files_per_folder': 3,
            'operation_mode': 'copy',
            'rename_mode': 'keep',
            'conflict_mode': 'auto_rename',
            'custom_prefix': ''
        }
        core.process(config)

        # 3. 验证结果
        # Merged_1
        files1 = os.listdir(m1)
        self.assertEqual(len(files1), 3, f"Merged_1 应有 3 个文件，实际 {len(files1)}")
        
        # Merged_2
        m2 = os.path.join(self.target_dir, "Merged_2")
        self.assertTrue(os.path.exists(m2), "Merged_2 应该被创建")
        files2 = os.listdir(m2)
        self.assertEqual(len(files2), 3, f"Merged_2 应有 3 个文件，实际 {len(files2)}")

        # Merged_3
        files3 = os.listdir(m3)
        self.assertEqual(len(files3), 3, f"Merged_3 应有 3 个文件，实际 {len(files3)}")

        # Merged_4
        m4 = os.path.join(self.target_dir, "Merged_4")
        self.assertTrue(os.path.exists(m4), "Merged_4 应该被创建")
        files4 = os.listdir(m4)
        self.assertEqual(len(files4), 3, f"Merged_4 应有 3 个文件，实际 {len(files4)}")

        # Merged_5
        m5 = os.path.join(self.target_dir, "Merged_5")
        self.assertTrue(os.path.exists(m5), "Merged_5 应该被创建")
        files5 = os.listdir(m5)
        self.assertEqual(len(files5), 1, f"Merged_5 应有 1 个文件，实际 {len(files5)}")

if __name__ == '__main__':
    unittest.main()
